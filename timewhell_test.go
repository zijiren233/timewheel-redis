package timewheel_test

import (
	"os"
	"testing"
	"time"

	timewheel "github.com/burybell/cluster-timewheel"
	"github.com/redis/go-redis/v9"
)

func TestTimeWhell(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Username: os.Getenv("REDIS_USER"),
		Password: os.Getenv("REDIS_PASS"),
		DB:       0,
	})
	tw := timewheel.NewTimeWheel(rdb, "test-timewheel")
	err := tw.AddTimer("test5", time.Second*5, []byte("test5"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test5")
	err = tw.AddTimer("test65", time.Second*65, []byte("test65"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test65")
	go tw.Run()
	defer tw.Stop()
	err = tw.AddTimer("test35", time.Second*35, []byte("test35"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add timer test35")
	now := time.Now()
	job := <-tw.JobChan()
	t.Logf("cost time: %v", time.Since(now))
	if job.Id != "test5" {
		t.Error("job id not match")
	}
	if string(job.Data) != "test5" {
		t.Errorf("job data not match, got: %s", job.Data)
	}
	job = <-tw.JobChan()
	t.Logf("cost time: %v", time.Since(now))
	if job.Id != "test35" {
		t.Error("job id not match")
	}
	if string(job.Data) != "test35" {
		t.Errorf("job data not match, got: %s", job.Data)
	}
	job = <-tw.JobChan()
	t.Logf("cost time: %v", time.Since(now))
	if job.Id != "test65" {
		t.Error("job id not match")
	}
	if string(job.Data) != "test65" {
		t.Errorf("job data not match, got: %s", job.Data)
	}
	t.Logf("cost time: %v", time.Since(now))
}
