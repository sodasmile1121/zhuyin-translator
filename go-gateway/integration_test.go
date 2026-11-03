package main

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"go-gateway/db"

	"github.com/redis/go-redis/v9"
)

// These tests need real services and recreate the "jobs" table and flush the Redis database they point at,
// so they only run when INTEGRATION_DATABASE_URL and INTEGRATION_REDIS_ADDR are set (CI provides both).
func integrationEnv(t *testing.T) (*sql.DB, *redis.Client) {
	t.Helper()
	dsn, redisAddr := os.Getenv("INTEGRATION_DATABASE_URL"), os.Getenv("INTEGRATION_REDIS_ADDR")
	if dsn == "" || redisAddr == "" {
		t.Skip("set INTEGRATION_DATABASE_URL and INTEGRATION_REDIS_ADDR to run integration tests")
	}

	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	schema, err := os.ReadFile("sql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec("DROP TABLE IF EXISTS jobs"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(string(schema)); err != nil {
		t.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return conn, rdb
}

func insertJob(t *testing.T, conn *sql.DB, jobID, status, inputURL string, age time.Duration) {
	t.Helper()
	_, err := conn.Exec(
		`INSERT INTO jobs (job_id, status, s3_input_url, created_at) VALUES ($1, $2, $3, CURRENT_TIMESTAMP - make_interval(secs => $4::float8))`,
		jobID, status, inputURL, age.Seconds(),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_ListStuckJobsOnlyReturnsOldPendingJobs(t *testing.T) {
	conn, _ := integrationEnv(t)
	insertJob(t, conn, "old-pending", "pending", "https://b.s3.amazonaws.com/inputs/a.pdf", 10*time.Minute)
	insertJob(t, conn, "new-pending", "pending", "https://b.s3.amazonaws.com/inputs/b.pdf", 10*time.Second)
	insertJob(t, conn, "old-done", "success", "https://b.s3.amazonaws.com/inputs/c.pdf", 10*time.Minute)

	repo := &SQLJobRepository{queries: db.New(conn)}
	rows, err := repo.ListStuckJobs(context.Background(), 5*time.Minute, 100)

	if err != nil || len(rows) != 1 || rows[0].JobID != "old-pending" {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestIntegration_ReconcilerRepublishesStuckJobOnce(t *testing.T) {
	conn, rdb := integrationEnv(t)
	insertJob(t, conn, "stuck", "pending", "https://b.s3.amazonaws.com/inputs/stuck_a.pdf?X-Amz-Expires=900", 10*time.Minute)

	r := &Reconciler{
		repo:      &SQLJobRepository{queries: db.New(conn)},
		storage:   &MockStorageService{},
		queue:     &RedisQueueService{rdb: rdb},
		olderThan: 5 * time.Minute,
		cooldown:  time.Minute,
		batchSize: 100,
	}
	ctx := context.Background()

	if n, err := r.ReconcileOnce(ctx); err != nil || n != 1 {
		t.Fatalf("first run: n=%d err=%v", n, err)
	}
	entries, err := rdb.XRange(ctx, TaskStreamKey, "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	if entries[0].Values["job_id"] != "stuck" || entries[0].Values["file_path"] != "https://mock-s3.com/inputs/stuck_a.pdf" {
		t.Errorf("unexpected task: %v", entries[0].Values)
	}

	if n, err := r.ReconcileOnce(ctx); err != nil || n != 0 {
		t.Errorf("second run must be deduplicated: n=%d err=%v", n, err)
	}
	if length, _ := rdb.XLen(ctx, TaskStreamKey).Result(); length != 1 {
		t.Errorf("task was published %d times", length)
	}
}

func TestIntegration_JobEventUpdatesPostgres(t *testing.T) {
	conn, rdb := integrationEnv(t)
	insertJob(t, conn, "j1", "pending", "https://b.s3.amazonaws.com/inputs/j1.pdf", time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := &SQLJobRepository{queries: db.New(conn)}
	srv := &Server{id: "it", ctx: ctx, rdb: rdb, hub: NewHub(), repo: repo, queue: &MockQueueService{}}
	srv.startRedisSub()
	time.Sleep(100 * time.Millisecond) // let the consumer group exist before the event is published

	err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: StreamKey, Values: map[string]interface{}{
		"job_id": "j1", "status": "success", "output_path": "outputs/j1_translated.pdf",
	}}).Err()
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		job, err := repo.GetJob(ctx, "j1")
		if err == nil && job.Status == "success" {
			if job.S3OutputUrl.String != "outputs/j1_translated.pdf" {
				t.Errorf("output not stored: %+v", job)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job status never became success: %+v err=%v", job, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)
	pending, err := rdb.XPending(ctx, StreamKey, GroupName).Result()
	if err != nil || pending.Count != 0 {
		t.Errorf("event must be acknowledged: %+v err=%v", pending, err)
	}
}
