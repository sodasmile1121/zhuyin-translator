package main

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// WorkerGroupName is the consumer group the Python workers read stream:task with (TASK_GROUP_NAME in worker.py).
const WorkerGroupName = "python-worker-group"

var (
	taskQueueBacklogDesc = prometheus.NewDesc(
		"task_queue_backlog",
		"Tasks in stream:task that no worker has taken yet (the queue length).",
		nil, nil,
	)
	taskQueueInFlightDesc = prometheus.NewDesc(
		"task_queue_in_flight",
		"Tasks a worker has taken but not acknowledged yet (being processed, or waiting to be retried).",
		nil, nil,
	)
)

// workerGroupCounts picks the worker group out of XINFO GROUPS. Redis reports the lag as -1 when it cannot work it
// out (for example after entries were deleted from the middle of the stream); then backlogKnown is false and no
// backlog is exported, instead of a 0 that would look like an empty queue.
func workerGroupCounts(groups []redis.XInfoGroup) (backlog, inFlight float64, backlogKnown, found bool) {
	for _, g := range groups {
		if g.Name == WorkerGroupName {
			return float64(g.Lag), float64(g.Pending), g.Lag >= 0, true
		}
	}
	return 0, 0, false, false
}

// queueCollector asks Redis for the queue state each time Prometheus scrapes, so there is no background loop to keep
// in sync. XLEN is deliberately not exported: the stream is never trimmed, so its length only ever grows.
type queueCollector struct {
	rdb *redis.Client
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- taskQueueBacklogDesc
	ch <- taskQueueInFlightDesc
}

// Collect exports nothing when Redis cannot be reached, so the graph shows a gap rather than a made-up value.
func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	groups, err := c.rdb.XInfoGroups(ctx, TaskStreamKey).Result()
	if err != nil {
		return
	}
	backlog, inFlight, backlogKnown, found := workerGroupCounts(groups)
	if !found {
		return
	}
	if backlogKnown {
		ch <- prometheus.MustNewConstMetric(taskQueueBacklogDesc, prometheus.GaugeValue, backlog)
	}
	ch <- prometheus.MustNewConstMetric(taskQueueInFlightDesc, prometheus.GaugeValue, inFlight)
}
