package timewheel

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	zstd "github.com/klauspost/compress/zstd"
	redis "github.com/redis/go-redis/v9"
)

type OptionFunc func(*TimeWheel)

func WithInterval(interval time.Duration) OptionFunc {
	return func(options *TimeWheel) {
		options.interval = interval
	}
}

func WithSlotNums(slotNums int64) OptionFunc {
	return func(options *TimeWheel) {
		options.slotNums = slotNums
	}
}

func WithContext(ctx context.Context) OptionFunc {
	return func(options *TimeWheel) {
		options.ctx = ctx
	}
}

func WithEnableEvalRO(enableEvalRO bool) OptionFunc {
	return func(options *TimeWheel) {
		options.enableEvalRO = enableEvalRO
	}
}

type Timer struct {
	Id      string
	Payload []byte
}

type TimeWheel struct {
	name         string
	interval     time.Duration
	slotNums     int64
	currentPos   int64
	client       *redis.Client
	ctx          context.Context
	cancel       context.CancelFunc
	donechan     chan *Timer
	enableEvalRO bool
}

func NewTimeWheel(client *redis.Client, name string, opts ...OptionFunc) *TimeWheel {
	if client == nil || len(name) == 0 {
		panic("client and name must not be nil")
	}

	timewheel := &TimeWheel{
		client:   client,
		name:     name,
		interval: time.Second,
		slotNums: 60,
		ctx:      context.Background(),
		donechan: make(chan *Timer, 100),
	}

	for _, opt := range opts {
		opt(timewheel)
	}

	if timewheel.slotNums < 1 {
		panic("slotNums must be greater than 0")
	}

	timewheel.currentPos = rand.Int63n(timewheel.slotNums)

	timewheel.ctx, timewheel.cancel = context.WithCancel(timewheel.ctx)

	return timewheel
}

func (wheel *TimeWheel) DoneChan() <-chan *Timer {
	return wheel.donechan
}

func (wheel *TimeWheel) getTargetLockExpire() time.Duration {
	if wheel.slotNums > 1 {
		return time.Duration(wheel.slotNums-1) * wheel.interval
	}
	return time.Duration(wheel.slotNums) * wheel.interval
}

func (wheel *TimeWheel) getTargetExpire(delay time.Duration) time.Duration {
	return time.Duration(wheel.slotNums)*wheel.interval*2 + delay
}

var delTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local t = name .. ":t:" .. id
local idx = redis.call("HGET", t, "idx")
if not idx then
	return false
end
local s = name .. ":s:" .. idx
redis.call("SREM", s, id)
redis.call("DEL", t)
return true
`)

func (wheel *TimeWheel) delTarget(id string) error {
	return delTargetScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
		},
	).Err()
}

var autoClockTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local idx = KEYS[3]
local tl = name .. ":tl:" .. id
if not redis.call("SET", tl, "", "NX", "EX", ARGV[1]) then
	return {}
end
local t = name .. ":t:" .. id
local circle = redis.call("HINCRBY", t, "circle", -1)
if not circle then
	local s = name .. ":s:" .. idx
	redis.call("SREM", s, id)
	return {}
end
if circle >= 0 then
	return {}
end
local results = redis.call("HMGET", t, "idx", "payload")
if not results or #results ~= 2 then
	return {}
end
local s = name .. ":s:" .. results[1]
redis.call("SREM", s, id)
redis.call("DEL", t)
return {results[2]}
`)

type decrTargetResp struct {
	done    bool
	payload []byte
}

func parseDecrTargetResp(ss []string) (*decrTargetResp, error) {
	resp := &decrTargetResp{}
	if len(ss) == 0 {
		return resp, nil
	}
	if len(ss) != 1 {
		return nil, fmt.Errorf("decr target response length invalid: %v", ss)
	}
	resp.done = true

	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	payload, err := dec.DecodeAll(stringToBytes(ss[0]), make([]byte, 0, len(ss[0])))
	if err != nil {
		return nil, fmt.Errorf("decompress payload failed: %w", err)
	}

	resp.payload = payload
	return resp, nil
}

func (wheel *TimeWheel) autoClockTarget(idx int64, id string) (*decrTargetResp, error) {
	ss, err := autoClockTargetScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
			strconv.FormatInt(idx, 10),
		},
		int(wheel.getTargetLockExpire().Seconds()),
	).StringSlice()
	if err != nil {
		return nil, err
	}
	return parseDecrTargetResp(ss)
}

var getNeedClockTargetScript = redis.NewScript(`
local name = KEYS[1]
local idx = KEYS[2]
local s = name .. ":s:" .. idx
local ts = redis.call("SMEMBERS", s)
local res = {}
for _, v in ipairs(ts) do
	local tl = name .. ":tl:" .. v
	if redis.call("EXISTS", tl) == 0 then
		table.insert(res, v)
	end
end
return res
`)

func (wheel *TimeWheel) getNeedClockTargets(idx int64) ([]string, error) {
	var f func(ctx context.Context, c redis.Scripter, keys []string, args ...interface{}) *redis.Cmd
	if wheel.enableEvalRO {
		f = getNeedClockTargetScript.RunRO
	} else {
		f = getNeedClockTargetScript.Run
	}
	ts, err := f(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			strconv.FormatInt(idx, 10),
		},
	).StringSlice()
	if err != nil {
		return nil, err
	}
	return ts, nil
}

func (wheel *TimeWheel) proxy(ctx context.Context, idx int64, it func(idx int64, id string)) {
	ts, err := wheel.getNeedClockTargets(idx)
	if err != nil {
		return
	}

	// TODO: goroutine pool + wait group
	for _, t := range ts {
		select {
		case <-ctx.Done():
			return
		default:
			it(idx, t)
		}
	}
}

type addTimerOptions struct {
	force   bool
	payload []byte
}

type AddTimerOption func(*addTimerOptions)

// will delete old target if exists
func WithForce() AddTimerOption {
	return func(options *addTimerOptions) {
		options.force = true
	}
}

func WithPayload(payload []byte) AddTimerOption {
	return func(options *addTimerOptions) {
		options.payload = payload
	}
}

var putTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local t = name .. ":t:" .. id
if redis.call("EXISTS", t) == 1 then
	return {err="already exists"}
end
local idx = ARGV[2]
redis.call("HMSET", t, "circle", ARGV[1], "idx", idx, "payload", ARGV[3])
redis.call("EXPIRE", t, ARGV[4])
local s = name .. ":s:" .. idx
redis.call("SADD", s, id)
local lock = name .. ":tl:" .. id
redis.call("SET", lock, "", "EX", ARGV[5])
return true
`)

var forcePutTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local t = name .. ":t:" .. id
local oldIdx = redis.call("HGET", t, "idx")
if oldIdx then
	local oldS = name .. ":s:" .. oldIdx
	redis.call("SREM", oldS, id)
end

local idx = ARGV[2]
redis.call("HMSET", t, "circle", ARGV[1], "idx", idx, "payload", ARGV[3])
redis.call("EXPIRE", t, ARGV[4])
local s = name .. ":s:" .. idx
redis.call("SADD", s, id)
local lock = name .. ":tl:" .. id
redis.call("SET", lock, "", "EX", ARGV[5])
return true
`)

func (wheel *TimeWheel) AddTimer(id string, delay time.Duration, opts ...AddTimerOption) error {
	if delay < 0 {
		return fmt.Errorf("delay must be greater than 0")
	}

	var options addTimerOptions
	for _, opt := range opts {
		opt(&options)
	}

	if delay == 0 {
		wheel.donechan <- &Timer{
			Id:      id,
			Payload: options.payload,
		}
	}

	currentPos := atomic.LoadInt64(&wheel.currentPos)
	delaySec := int64(delay.Seconds())
	circle := delaySec / int64(wheel.interval.Seconds()) / wheel.slotNums
	if delaySec/int64(wheel.interval.Seconds())%wheel.slotNums == 0 {
		circle--
	}
	idx := (currentPos + delaySec/int64(wheel.interval.Seconds())) % wheel.slotNums

	var preLock time.Duration
	if idx <= currentPos {
		preLock = wheel.interval * time.Duration(60-currentPos+idx)
	} else {
		preLock = wheel.interval * time.Duration(idx-currentPos)
	}

	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()

	var script *redis.Script
	if options.force {
		script = forcePutTargetScript
	} else {
		script = putTargetScript
	}

	err := script.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
		},
		circle,
		idx,
		enc.EncodeAll(options.payload, make([]byte, 0, len(options.payload))),
		int(wheel.getTargetExpire(delay).Seconds()),
		int(preLock.Seconds()),
	).Err()
	if err != nil {
		return fmt.Errorf("put target failed: %w", err)
	}
	return nil
}

func (wheel *TimeWheel) RemoveTimer(id string) error {
	return wheel.delTarget(id)
}

func (wheel *TimeWheel) Run() {
	ticker := time.NewTicker(wheel.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			go wheel.exec(
				wheel.ctx,
				wheel.tickPos(),
			)
		case <-wheel.ctx.Done():
			return
		}
	}
}

func (wheel *TimeWheel) tickPos() int64 {
	if !atomic.CompareAndSwapInt64(&wheel.currentPos, wheel.slotNums-1, 0) {
		return atomic.AddInt64(&wheel.currentPos, 1) - 1
	} else {
		return wheel.slotNums - 1
	}
}

func (wheel *TimeWheel) exec(ctx context.Context, idx int64) {
	wheel.proxy(ctx, idx, func(idx int64, id string) {
		resp, err := wheel.autoClockTarget(idx, id)
		if err != nil {
			return
		}
		if !resp.done {
			return
		}

		wheel.donechan <- &Timer{
			Id:      id,
			Payload: resp.payload,
		}
	})
}

func (wheel *TimeWheel) Stop() {
	wheel.cancel()
}
