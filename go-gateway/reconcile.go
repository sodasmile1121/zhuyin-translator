package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"
)

const presignedInputTTL = 15 * time.Minute

// Reconciler repairs the gateway's dual-write gap: handleUpload stores the file in S3, writes the job to
// Postgres and only then publishes the task, so a crash in between leaves a job that stays "pending" forever.
// It re-publishes the task of every job that has been pending for too long.
type Reconciler struct {
	repo      JobRepository
	storage   StorageService
	queue     QueueService
	olderThan time.Duration
	interval  time.Duration
	cooldown  time.Duration
	batchSize int32
}

func newReconcilerFromEnv(repo JobRepository, storage StorageService, queue QueueService) *Reconciler {
	return &Reconciler{
		repo:      repo,
		storage:   storage,
		queue:     queue,
		olderThan: envDuration("RECONCILE_OLDER_THAN", 5*time.Minute),
		interval:  envDuration("RECONCILE_INTERVAL", time.Minute),
		cooldown:  envDuration("RECONCILE_COOLDOWN", 10*time.Minute),
		batchSize: 100,
	}
}

func envDuration(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("Ignoring invalid %s=%q, using %s\n", name, raw, fallback)
		return fallback
	}
	return d
}

// Run blocks until ctx is cancelled. An interval of 0 or less disables the reconciler.
func (r *Reconciler) Run(ctx context.Context) {
	if r.interval <= 0 {
		log.Println("Reconciler disabled (RECONCILE_INTERVAL <= 0)")
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := r.ReconcileOnce(ctx); err != nil {
				log.Printf("Reconcile error: %v\n", err)
			} else if n > 0 {
				log.Printf("Reconciler re-enqueued %d stuck job(s)\n", n)
			}
		}
	}
}

// ReconcileOnce re-publishes stuck jobs and returns how many it re-enqueued.
func (r *Reconciler) ReconcileOnce(ctx context.Context) (int, error) {
	jobs, err := r.repo.ListStuckJobs(ctx, r.olderThan, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("list stuck jobs: %w", err)
	}

	requeued := 0
	for _, job := range jobs {
		s3Key, err := s3KeyFromPresignedURL(job.S3InputUrl)
		if err != nil {
			log.Printf("Reconciler: skipping job %s: %v\n", job.JobID, err)
			continue
		}
		// A job may be pending only because the queue is long. Do not enqueue it again every minute.
		first, err := r.queue.TryMarkRequeued(ctx, job.JobID, r.cooldown)
		if err != nil {
			log.Printf("Reconciler: dedupe check failed for job %s: %v\n", job.JobID, err)
			continue
		}
		if !first {
			continue
		}
		// The URL stored at upload time has expired by now, so sign a fresh one.
		freshURL, err := r.storage.GeneratePresignedURL(ctx, s3Key, presignedInputTTL)
		if err != nil {
			log.Printf("Reconciler: cannot presign %s for job %s: %v\n", s3Key, job.JobID, err)
			continue
		}
		if err := r.queue.PublishTask(ctx, job.JobID, freshURL); err != nil {
			log.Printf("Reconciler: cannot publish job %s: %v\n", job.JobID, err)
			continue
		}
		requeued++
	}
	return requeued, nil
}

// s3KeyFromPresignedURL recovers the object key from a virtual-hosted-style presigned URL
// (https://<bucket>.s3.<region>.amazonaws.com/<key>?X-Amz-...), which is what the gateway stores.
func s3KeyFromPresignedURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse input url: %w", err)
	}
	key := strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", fmt.Errorf("no object key in input url")
	}
	return key, nil
}
