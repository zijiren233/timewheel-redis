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

func WithBatchSize(batchSize int) OptionFunc {
	return func(options *TimeWheel) {
		if batchSize < 1 {
			return
		}
		options.batchSize = batchSize
	}
}

func WithRetry(retry time.Duration) OptionFunc {
	return func(options *TimeWheel) {
		options.retry = retry
	}
}

type Timer struct {
	Id      string
	Payload []byte
}

type Callback func(*Timer) bool

type TimeWheel struct {
	name         string
	interval     time.Duration
	slotNums     int64
	currentPos   int64
	client       *redis.Client
	ctx          context.Context
	cancel       context.CancelFunc
	callback     Callback
	enableEvalRO bool
	batchSize    int
	retry        time.Duration
}

func NewTimeWheel(client *redis.Client, name string, callback Callback, opts ...OptionFunc) *TimeWheel {
	if client == nil || len(name) == 0 {
		panic("client and name must not be nil")
	}

	timewheel := &TimeWheel{
		client:    client,
		name:      name,
		interval:  time.Second,
		slotNums:  60,
		ctx:       context.Background(),
		callback:  callback,
		batchSize: 3,
		retry:     time.Second * 10,
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
local d = name .. ":d"
redis.call("SREM", d, id)
local dl = name .. ":dl:" .. id
redis.call("DEL", dl)
return true
`)

var getDoneTargetsPayloadScript = redis.NewScript(`
local name = KEYS[1]
local idx = KEYS[2]
local expire = KEYS[3]
local retry = KEYS[4]
local results = {}
for _, id in ipairs(ARGV) do
	local skip = false
	local t = name .. ":t:" .. id
	if redis.call("EXISTS", t) == 0 then
		skip = true
		redis.call("SREM", name .. ":d", id)
	end
	local dl = name .. ":dl:" .. id
	if not redis.call("SET", dl, "", "NX", "EX", retry) then
		skip = true
	end
	if not skip then
		local r = redis.call("HMGET", t, "payload")
		if r then
			redis.call("EXPIRE", t, expire)
			table.insert(results, id)
			table.insert(results, r[1])
		end
	end
end
return results
`)

func parseDecrTargetsResp(ss []string) (map[string][]byte, error) {
	if len(ss)%2 != 0 {
		return nil, fmt.Errorf("decr targets response length invalid: %v", ss)
	}

	results := make(map[string][]byte, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		id := ss[i]
		dec, _ := zstd.NewReader(nil)
		defer dec.Close()
		payload, err := dec.DecodeAll(stringToBytes(ss[i+1]), make([]byte, 0, len(ss[i+1])))
		if err != nil {
			// return nil, fmt.Errorf("decompress payload failed: %w", err)
			continue
		}
		results[id] = payload
	}

	return results, nil
}

var deleteDoneTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local d = name .. ":d"
if redis.call("SREM", d, id) == 0 then
	return false
end
local dl = name .. ":dl:" .. id
redis.call("DEL", dl)
local t = name .. ":t:" .. id
redis.call("DEL", t)
return true
`)

func (wheel *TimeWheel) deleteDoneTarget(id string) error {
	return deleteDoneTargetScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
		},
	).Err()
}

func slice2interface[T any](s []T) []interface{} {
	var r []interface{} = make([]interface{}, len(s))
	for i, v := range s {
		r[i] = v
	}
	return r
}

func (wheel *TimeWheel) getDoneTargetsPayload(idx int64, ids []string) (map[string][]byte, error) {
	ss, err := getDoneTargetsPayloadScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			strconv.FormatInt(idx, 10),
			strconv.FormatInt(int64(wheel.getTargetLockExpire().Seconds()), 10),
			strconv.FormatInt(int64(wheel.retry.Seconds()), 10),
		},
		slice2interface(ids)...,
	).StringSlice()
	if err != nil {
		return nil, err
	}

	return parseDecrTargetsResp(ss)
}

var clockAndGetDoneTargetScript = redis.NewScript(`
local name = KEYS[1]
local idx = KEYS[2]
local s = name .. ":s:" .. idx
local ts = redis.call("SMEMBERS", s)
for _, v in ipairs(ts) do
	local tl = name .. ":tl:" .. v
	if redis.call("EXISTS", tl) == 0 then
		local d = name .. ":d"
		redis.call("SADD", d, v)
		redis.call("SREM", s, v)
	end
end
local res = {}
local d = name .. ":d"
local ds = redis.call("SMEMBERS", d)
for _, v in ipairs(ds) do
	local dl = name .. ":dl:" .. v
	if redis.call("EXISTS", dl) == 0 then
		table.insert(res, v)
	end
end
return res
`)

func (wheel *TimeWheel) clockAndGetDoneTargets(idx int64) ([]string, error) {
	var f func(ctx context.Context, c redis.Scripter, keys []string, args ...interface{}) *redis.Cmd
	if wheel.enableEvalRO {
		f = clockAndGetDoneTargetScript.RunRO
	} else {
		f = clockAndGetDoneTargetScript.Run
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

func (wheel *TimeWheel) proxy(ctx context.Context, idx int64, run func(idx int64, ids []string)) {
	ts, err := wheel.clockAndGetDoneTargets(idx)
	if err != nil {
		return
	}

	if len(ts) == 0 {
		return
	}

	for _, t := range splitIntoBatches(ts, wheel.batchSize) {
		select {
		case <-ctx.Done():
			return
		default:
			run(idx, t)
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
local idx = ARGV[1]
redis.call("HMSET", t, "idx", idx, "payload", ARGV[2])
redis.call("EXPIRE", t, ARGV[3])
local s = name .. ":s:" .. idx
redis.call("SADD", s, id)
local tl = name .. ":tl:" .. id
redis.call("SET", tl, "", "EX", ARGV[4])
return true
`)

var forcePutTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local t = name .. ":t:" .. id
local oldIdx = redis.call("HGET", t, "idx")
if oldIdx then
	local d = name .. ":d"
	if redis.call("SREM", d, id) then
		local dl = name .. ":dl:" .. id
		redis.call("DEL", dl)
	else
		local oldS = name .. ":s:" .. oldIdx
		redis.call("SREM", oldS, id)
	end
end

local idx = ARGV[1]
redis.call("HMSET", t, "idx", idx, "payload", ARGV[2])
redis.call("EXPIRE", t, ARGV[3])
local s = name .. ":s:" .. idx
redis.call("SADD", s, id)
local tl = name .. ":tl:" .. id
redis.call("SET", tl, "", "EX", ARGV[4])
return true
`)

func (wheel *TimeWheel) AddTimer(id string, delay time.Duration, opts ...AddTimerOption) error {
	if delay < 0 {
		return fmt.Errorf("delay must be greater than 0")
	}
	if delay <= wheel.interval {
		delay = wheel.interval
	}

	var options addTimerOptions
	for _, opt := range opts {
		opt(&options)
	}

	currentPos := atomic.LoadInt64(&wheel.currentPos)
	delaySec := int64(delay.Seconds())
	idx := (currentPos + delaySec/int64(wheel.interval.Seconds())) % wheel.slotNums

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
		idx,
		enc.EncodeAll(options.payload, make([]byte, 0, len(options.payload))),
		int(wheel.getTargetExpire(delay).Seconds()),
		delaySec,
	).Err()
	if err != nil {
		return fmt.Errorf("put target failed: %w", err)
	}
	return nil
}

func (wheel *TimeWheel) RemoveTimer(id string) error {
	return delTargetScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
		},
	).Err()
}

func (wheel *TimeWheel) GetTimer(id string) (time.Duration, error) {
	t, err := wheel.client.TTL(
		wheel.ctx,
		fmt.Sprintf("%s:tl:%s", wheel.name, id),
	).Result()
	if err != nil {
		return 0, err
	}
	return t, nil
}

var resetTimerScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local t = name .. ":t:" .. id
local idx = redis.call("HGET", t, "idx")
if not idx then
	return false
end
local d = name .. ":d"
if redis.call("SISMEMBER", d, id) == 1 then
	return false
end
local tl = name .. ":tl:" .. id
redis.call("SET", tl, "", "EX", ARGV[2])
local s = name .. ":s:" .. idx
redis.call("SREM", s, id)
local s = name .. ":s:" .. ARGV[1]
redis.call("SADD", s, id)
redis.call("HMSET", t, "idx", ARGV[1])
return true
`)

func (wheel *TimeWheel) ResetTimer(id string, delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("delay must be greater than 0")
	}
	if delay <= wheel.interval {
		delay = wheel.interval
	}

	delaySec := int64(delay.Seconds())
	idx := (atomic.LoadInt64(&wheel.currentPos) + delaySec/int64(wheel.interval.Seconds())) % wheel.slotNums

	return resetTimerScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
		},
		idx,
		delaySec,
	).Err()
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
	wheel.proxy(ctx, idx, func(idx int64, ids []string) {
		resp, err := wheel.getDoneTargetsPayload(idx, ids)
		if err != nil {
			return
		}
		for id, payload := range resp {
			_ = wheel.call(&Timer{
				Id:      id,
				Payload: payload,
			})
		}
	})
}

func (wheel *TimeWheel) call(t *Timer) error {
	if wheel.callback(t) {
		return wheel.deleteDoneTarget(t.Id)
	}
	return nil
}

func (wheel *TimeWheel) Stop() {
	wheel.cancel()
}
