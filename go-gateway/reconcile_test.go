package main

import (
	"context"
	"fmt"
	"go-gateway/db"
	"testing"
	"time"
)

func TestS3KeyFromPresignedURL(t *testing.T) {
	key, err := s3KeyFromPresignedURL("https://bucket.s3.us-east-1.amazonaws.com/inputs/abc_my%20file.pdf?X-Amz-Signature=zzz")
	if err != nil || key != "inputs/abc_my file.pdf" {
		t.Errorf("got %q, %v", key, err)
	}
	if _, err := s3KeyFromPresignedURL("https://bucket.s3.amazonaws.com/"); err == nil {
		t.Error("a URL without a key must be rejected")
	}
}

type publishedTask struct{ jobID, filePath string }

func newTestReconciler(rows []db.ListStuckJobsRow, queue *MockQueueService, storage *MockStorageService) *Reconciler {
	return &Reconciler{
		repo: &MockJobRepository{
			ListStuckJobsFunc: func(ctx context.Context, olderThan time.Duration, limit int32) ([]db.ListStuckJobsRow, error) {
				return rows, nil
			},
		},
		storage:   storage,
		queue:     queue,
		olderThan: time.Minute,
		cooldown:  time.Minute,
		batchSize: 10,
	}
}

func TestReconcileOnce_RepublishesWithFreshURL(t *testing.T) {
	var published []publishedTask
	queue := &MockQueueService{
		PublishTaskFunc: func(ctx context.Context, jobID, filePath string) error {
			published = append(published, publishedTask{jobID, filePath})
			return nil
		},
	}
	rows := []db.ListStuckJobsRow{{JobID: "j1", S3InputUrl: "https://b.s3.amazonaws.com/inputs/j1_a.pdf?X-Amz-Expires=900"}}

	n, err := newTestReconciler(rows, queue, &MockStorageService{}).ReconcileOnce(context.Background())

	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	// The mock storage signs "https://mock-s3.com/<key>": proves the key was recovered and re-signed.
	if len(published) != 1 || published[0].jobID != "j1" || published[0].filePath != "https://mock-s3.com/inputs/j1_a.pdf" {
		t.Errorf("unexpected tasks: %+v", published)
	}
}

func TestReconcileOnce_SkipsJobsRequeuedRecently(t *testing.T) {
	published := 0
	queue := &MockQueueService{
		TryMarkRequeuedFunc: func(ctx context.Context, jobID string, ttl time.Duration) (bool, error) { return false, nil },
		PublishTaskFunc:     func(ctx context.Context, jobID, filePath string) error { published++; return nil },
	}
	rows := []db.ListStuckJobsRow{{JobID: "j1", S3InputUrl: "https://b.s3.amazonaws.com/inputs/j1.pdf"}}

	n, _ := newTestReconciler(rows, queue, &MockStorageService{}).ReconcileOnce(context.Background())

	if n != 0 || published != 0 {
		t.Errorf("a job inside its cooldown must not be published again (n=%d, published=%d)", n, published)
	}
}

func TestReconcileOnce_OneBadJobDoesNotBlockTheOthers(t *testing.T) {
	published := map[string]bool{}
	queue := &MockQueueService{
		PublishTaskFunc: func(ctx context.Context, jobID, filePath string) error {
			if jobID == "publish-fails" {
				return fmt.Errorf("redis down")
			}
			published[jobID] = true
			return nil
		},
	}
	rows := []db.ListStuckJobsRow{
		{JobID: "bad-url", S3InputUrl: "https://b.s3.amazonaws.com/"},
		{JobID: "publish-fails", S3InputUrl: "https://b.s3.amazonaws.com/inputs/x.pdf"},
		{JobID: "good", S3InputUrl: "https://b.s3.amazonaws.com/inputs/y.pdf"},
	}

	n, err := newTestReconciler(rows, queue, &MockStorageService{}).ReconcileOnce(context.Background())

	if err != nil || n != 1 || !published["good"] {
		t.Errorf("n=%d err=%v published=%v", n, err, published)
	}
}

func TestReconcileOnce_ListError(t *testing.T) {
	r := newTestReconciler(nil, &MockQueueService{}, &MockStorageService{})
	r.repo = &MockJobRepository{
		ListStuckJobsFunc: func(ctx context.Context, olderThan time.Duration, limit int32) ([]db.ListStuckJobsRow, error) {
			return nil, fmt.Errorf("db down")
		},
	}
	if _, err := r.ReconcileOnce(context.Background()); err == nil {
		t.Error("expected the database error to be returned")
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("X_TEST_DURATION", "90s")
	if d := envDuration("X_TEST_DURATION", time.Hour); d != 90*time.Second {
		t.Errorf("got %s", d)
	}
	t.Setenv("X_TEST_DURATION", "nonsense")
	if d := envDuration("X_TEST_DURATION", time.Hour); d != time.Hour {
		t.Errorf("invalid value must fall back, got %s", d)
	}
	if d := envDuration("X_TEST_UNSET_DURATION", time.Minute); d != time.Minute {
		t.Errorf("unset must fall back, got %s", d)
	}
}
