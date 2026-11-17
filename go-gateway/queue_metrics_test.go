package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

func TestWorkerGroupCounts(t *testing.T) {
	groups := []redis.XInfoGroup{
		{Name: "go-gateway-group", Lag: 99, Pending: 99},
		{Name: WorkerGroupName, Lag: 5, Pending: 2},
	}
	backlog, inFlight, known, found := workerGroupCounts(groups)
	if !found || !known || backlog != 5 || inFlight != 2 {
		t.Errorf("got backlog=%v inFlight=%v known=%v found=%v", backlog, inFlight, known, found)
	}

	if _, _, _, found := workerGroupCounts(groups[:1]); found {
		t.Error("another group must not be mistaken for the worker group")
	}

	_, inFlight, known, found = workerGroupCounts([]redis.XInfoGroup{{Name: WorkerGroupName, Lag: -1, Pending: 3}})
	if !found || known || inFlight != 3 {
		t.Errorf("an unknown lag must not be reported as a number: known=%v inFlight=%v", known, inFlight)
	}
}

func TestQueueCollectorExportsTasksInFlight(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	for _, job := range []string{"a", "b", "c"} {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: TaskStreamKey, Values: map[string]interface{}{"job_id": job}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rdb.XGroupCreate(ctx, TaskStreamKey, WorkerGroupName, "0").Err(); err != nil {
		t.Fatal(err)
	}
	// one worker takes one task and has not acknowledged it
	if err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: WorkerGroupName, Consumer: "w1", Streams: []string{TaskStreamKey, ">"}, Count: 1}).Err(); err != nil {
		t.Fatal(err)
	}

	// only task_queue_in_flight is compared: miniredis computes the lag in a simplified way
	want := `
# HELP task_queue_in_flight Tasks a worker has taken but not acknowledged yet (being processed, or waiting to be retried).
# TYPE task_queue_in_flight gauge
task_queue_in_flight 1
`
	if err := testutil.CollectAndCompare(&queueCollector{rdb: rdb}, strings.NewReader(want), "task_queue_in_flight"); err != nil {
		t.Error(err)
	}
}

func TestQueueCollectorExportsNothingWhenRedisIsDown(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	mr.Close()

	if n := testutil.CollectAndCount(&queueCollector{rdb: rdb}); n != 0 {
		t.Errorf("expected no metrics while Redis is unreachable, got %d", n)
	}
}

// Against a real Redis the lag is computed properly. Needs INTEGRATION_REDIS_ADDR; it FLUSHDBs that database.
func TestIntegration_QueueCollectorReportsBacklogAndInFlight(t *testing.T) {
	addr := os.Getenv("INTEGRATION_REDIS_ADDR")
	if addr == "" {
		t.Skip("set INTEGRATION_REDIS_ADDR to run integration tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.XGroupCreateMkStream(ctx, TaskStreamKey, WorkerGroupName, "$").Err(); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"a", "b", "c"} {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: TaskStreamKey, Values: map[string]interface{}{"job_id": job}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: WorkerGroupName, Consumer: "w1", Streams: []string{TaskStreamKey, ">"}, Count: 1}).Err(); err != nil {
		t.Fatal(err)
	}

	want := `
# HELP task_queue_backlog Tasks in stream:task that no worker has taken yet (the queue length).
# TYPE task_queue_backlog gauge
task_queue_backlog 2
# HELP task_queue_in_flight Tasks a worker has taken but not acknowledged yet (being processed, or waiting to be retried).
# TYPE task_queue_in_flight gauge
task_queue_in_flight 1
`
	if err := testutil.CollectAndCompare(&queueCollector{rdb: rdb}, strings.NewReader(want)); err != nil {
		t.Error(err)
	}
}
