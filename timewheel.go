package timewheel

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	redsync "github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	zstd "github.com/klauspost/compress/zstd"
	redis "github.com/redis/go-redis/v9"
)

type Target struct {
	Id     string
	Circle int
	Idx    int
	Job    []byte
}

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

type Job struct {
	Id   string
	Data []byte
}

type TimeWheel struct {
	name       string
	interval   time.Duration
	slotNums   int64
	currentPos int64
	client     *redis.Client
	rsync      *redsync.Redsync
	ctx        context.Context
	cancel     context.CancelFunc
	jobchan    chan *Job
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
		jobchan:  make(chan *Job, 100),
		rsync:    redsync.New(goredis.NewPool(client)),
	}

	for _, opt := range opts {
		opt(timewheel)
	}

	if timewheel.slotNums < 1 {
		panic("slotNums must be greater than 0")
	}

	timewheel.ctx, timewheel.cancel = context.WithCancel(timewheel.ctx)

	return timewheel
}

func (wheel *TimeWheel) JobChan() <-chan *Job {
	return wheel.jobchan
}

func (wheel *TimeWheel) getTargetLockExpire() time.Duration {
	if wheel.slotNums > 1 {
		return time.Duration(wheel.slotNums-1) * wheel.interval
	}
	return time.Duration(wheel.slotNums) * wheel.interval
}

func (wheel *TimeWheel) formatSlotKey(index int64) string {
	return fmt.Sprintf("%s:s:%v", wheel.name, index)
}

var putTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local t = name .. ":t:" .. id
if redis.call("EXISTS", t) == 1 then
	return {err="already exists"}
end
local idx = ARGV[2]
redis.call("HMSET", t, "circle", ARGV[1], "idx", idx, "job", ARGV[3])
redis.call("EXPIRE", t, ARGV[4])
local s = name .. ":s:" .. idx
redis.call("SADD", s, id)
local lock = name .. ":tl:" .. id
redis.call("SET", lock, "", "EX", ARGV[5])
return true
`)

func (wheel *TimeWheel) getTargetExpire(delay time.Duration) time.Duration {
	return time.Duration(wheel.slotNums)*wheel.interval + delay
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
	_, err := delTargetScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
		},
	).Result()
	return err
}

var autoClockTargetScript = redis.NewScript(`
local name = KEYS[1]
local id = KEYS[2]
local tl = name .. ":tl:" .. id
if not redis.call("SET", tl, "", "NX", "EX", ARGV[1]) then
	return {}
end
local t = name .. ":t:" .. id
local circle = redis.call("HINCRBY", t, "circle", -1)
if not circle then
	return false
end
if circle < 0 then
	local idx = redis.call("HGET", t, "idx")
	local s = name .. ":s:" .. idx
	redis.call("SREM", s, id)
	local job = redis.call("HGET", t, "job")
	redis.call("DEL", t)
	return {job}
end
return {}
`)

type decrTargetResp struct {
	done bool
	job  []byte
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
	data, err := dec.DecodeAll(stringToBytes(ss[0]), make([]byte, 0, len(ss[0])))
	if err != nil {
		return nil, fmt.Errorf("decompress job failed: %w", err)
	}

	resp.job = data
	return resp, nil
}

func (wheel *TimeWheel) autoClockTarget(id string) (*decrTargetResp, error) {
	ss, err := autoClockTargetScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			id,
		},
		int(wheel.getTargetLockExpire().Seconds()),
	).StringSlice()
	if err != nil {
		return nil, err
	}
	return parseDecrTargetResp(ss)
}

func (wheel *TimeWheel) proxy(ctx context.Context, idx int64, it func(id string)) {
	i := wheel.client.SScan(
		ctx,
		wheel.formatSlotKey(idx),
		0,
		"",
		0,
	).Iterator()

	// TODO: goroutine pool + wait group
	for i.Next(wheel.ctx) {
		select {
		case <-ctx.Done():
			return
		default:
			it(i.Val())
		}
	}
}

func (wheel *TimeWheel) AddTimer(id string, delay time.Duration, job []byte) error {
	if delay < 0 {
		return fmt.Errorf("delay must be greater than 0")
	}

	currentPos := int(atomic.LoadInt64(&wheel.currentPos))
	delaySec := int64(delay.Seconds())
	target := &Target{
		Id:     id,
		Circle: int(delaySec / int64(wheel.interval.Seconds()) / int64(wheel.slotNums)),
		Idx:    (currentPos + int(delaySec)/int(wheel.interval.Seconds())) % int(wheel.slotNums),
		Job:    job,
	}

	var preLock time.Duration
	if target.Idx < currentPos {
		preLock = wheel.interval * time.Duration((60 - currentPos + target.Idx))
	} else {
		preLock = wheel.interval * time.Duration((target.Idx - currentPos))
	}

	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()

	err := putTargetScript.Run(
		wheel.ctx,
		wheel.client,
		[]string{
			wheel.name,
			target.Id,
		},
		target.Circle,
		target.Idx,
		enc.EncodeAll(job, make([]byte, 0, len(job))),
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
	mutex := wheel.rsync.NewMutex(
		fmt.Sprintf("%s:sl:%d", wheel.name, idx),
		redsync.WithExpiry(time.Duration(wheel.slotNums-1)*wheel.interval),
	)
	err := mutex.TryLockContext(ctx)
	if err != nil {
		return
	}
	defer func() { _, _ = mutex.Unlock() }()

	wheel.proxy(ctx, idx, func(id string) {
		resp, err := wheel.autoClockTarget(id)
		if err != nil {
			return
		}
		if !resp.done {
			return
		}

		wheel.jobchan <- &Job{
			Id:   id,
			Data: resp.job,
		}
	})
}

func (wheel *TimeWheel) Stop() {
	wheel.cancel()
}
