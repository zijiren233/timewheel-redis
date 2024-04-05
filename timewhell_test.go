package timewheel_test

import (
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	timewheel "github.com/zijiren233/timewheel-redis"
)

func TestTimeWhell(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Username: os.Getenv("REDIS_USER"),
		Password: os.Getenv("REDIS_PASS"),
		DB:       0,
	})
	ch := make(chan *timewheel.Timer, 10)
	firstFail := true
	tw := timewheel.NewTimeWheel(
		rdb, "test-timewheel",
		func(t *timewheel.Timer) bool {
			if firstFail {
				firstFail = false
				return false
			}
			ch <- t
			return true
		},
	)
	err := tw.AddTimer("test5", time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test5")

	err = tw.AddTimer("test5", time.Second*5, timewheel.WithForce(), timewheel.WithPayload([]byte("test5")))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("force add timer test5 with payload")

	err = tw.AddTimer("test65", time.Second*65, timewheel.WithPayload([]byte("test65")))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test65")

	err = tw.AddTimer("test60", time.Second*60, timewheel.WithPayload([]byte("test60")))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test60")

	go tw.Run()
	defer tw.Stop()

	err = tw.AddTimer("test35", time.Second*35)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test35")

	err = tw.ResetTimer("test35", time.Second*40)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reset timer test35 to 40s")

	now := time.Now()

	timer := <-ch
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test5" {
		t.Error("timer id not match")
	}
	if string(timer.Payload) != "test5" {
		t.Errorf("timer payload not match, got: %s", timer.Payload)
	}

	timer = <-ch
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test35" {
		t.Error("timer id not match")
	}

	d, err := tw.GetTimer("test60")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("get timer test60, duration: %v", d)

	timer = <-ch
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test60" {
		t.Error("timer id not match")
	}
	if string(timer.Payload) != "test60" {
		t.Errorf("timer payload not match, got: %s", timer.Payload)
	}

	timer = <-ch
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test65" {
		t.Error("timer id not match")
	}
	if string(timer.Payload) != "test65" {
		t.Errorf("timer payload not match, got: %s", timer.Payload)
	}

	t.Logf("cost time: %v", time.Since(now))
}
