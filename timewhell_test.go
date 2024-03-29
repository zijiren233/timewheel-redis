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
	tw := timewheel.NewTimeWheel(rdb, "test-timewheel")

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

	now := time.Now()

	timer := <-tw.DoneChan()
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test5" {
		t.Error("timer id not match")
	}
	if string(timer.Payload) != "test5" {
		t.Errorf("timer payload not match, got: %s", timer.Payload)
	}

	timer = <-tw.DoneChan()
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test35" {
		t.Error("timer id not match")
	}

	timer = <-tw.DoneChan()
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test60" {
		t.Error("timer id not match")
	}
	if string(timer.Payload) != "test60" {
		t.Errorf("timer payload not match, got: %s", timer.Payload)
	}

	timer = <-tw.DoneChan()
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test65" {
		t.Error("timer id not match")
	}
	if string(timer.Payload) != "test65" {
		t.Errorf("timer payload not match, got: %s", timer.Payload)
	}

	t.Logf("cost time: %v", time.Since(now))
}

func TestTimeWhell2(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Username: os.Getenv("REDIS_USER"),
		Password: os.Getenv("REDIS_PASS"),
		DB:       0,
	})
	tw := timewheel.NewTimeWheel(rdb, "test-timewheel2")

	err := tw.AddTimer("test60", time.Second*60, timewheel.WithForce(), timewheel.WithPayload([]byte("test60")))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test60")

	go tw.Run()
	defer tw.Stop()

	now := time.Now()

	timer := <-tw.DoneChan()
	t.Logf("cost time: %v", time.Since(now))
	if timer.Id != "test60" {
		t.Error("timer id not match")
	}
	if string(timer.Payload) != "test60" {
		t.Errorf("timer payload not match, got: %s", timer.Payload)
	}

	t.Logf("cost time: %v", time.Since(now))
}
