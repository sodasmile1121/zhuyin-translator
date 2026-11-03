package main

import (
	"context"
	"database/sql"
	"fmt"
	"go-gateway/db"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func newEventTestServer(t *testing.T, repo JobRepository) (*Server, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := rdb.XGroupCreateMkStream(ctx, StreamKey, GroupName, "$").Err(); err != nil {
		t.Fatal(err)
	}
	return &Server{id: "test", ctx: ctx, rdb: rdb, hub: NewHub(), repo: repo, queue: &MockQueueService{}}, rdb, mr
}

func addEvent(t *testing.T, rdb *redis.Client, values map[string]interface{}) {
	t.Helper()
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: StreamKey, Values: values}).Err(); err != nil {
		t.Fatal(err)
	}
}

func pendingCount(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	p, err := rdb.XPending(context.Background(), StreamKey, GroupName).Result()
	if err != nil {
		t.Fatal(err)
	}
	return p.Count
}

func TestHandleJobEvent_MalformedEventIsAcked(t *testing.T) {
	updated := false
	srv, rdb, _ := newEventTestServer(t, &MockJobRepository{
		UpdateJobStatusFunc: func(ctx context.Context, arg db.UpdateJobStatusParams) error {
			updated = true
			return nil
		},
	})
	addEvent(t, rdb, map[string]interface{}{"status": "success"}) // no job_id

	messages, err := srv.nextJobEvents()
	if err != nil || len(messages) != 1 {
		t.Fatalf("expected one event, got %v, %v", messages, err)
	}
	srv.handleJobEvent(messages[0])

	if updated {
		t.Error("a malformed event must not touch the database")
	}
	if n := pendingCount(t, rdb); n != 0 {
		t.Errorf("malformed event must be acknowledged, %d still pending", n)
	}
}

func TestHandleJobEvent_FailedEventWithEmptyOutputIsProcessed(t *testing.T) {
	var got db.UpdateJobStatusParams
	srv, rdb, _ := newEventTestServer(t, &MockJobRepository{
		UpdateJobStatusFunc: func(ctx context.Context, arg db.UpdateJobStatusParams) error {
			got = arg
			return nil
		},
	})
	addEvent(t, rdb, map[string]interface{}{"job_id": "j1", "status": "failed", "output_path": ""})

	messages, _ := srv.nextJobEvents()
	srv.handleJobEvent(messages[0])

	if got.Status != "failed" || got.JobID != "j1" || got.S3OutputUrl != (sql.NullString{}) {
		t.Errorf("unexpected update: %+v", got)
	}
	if n := pendingCount(t, rdb); n != 0 {
		t.Errorf("%d events still pending", n)
	}
}

func TestHandleJobEvent_DatabaseErrorKeepsEventPendingAndItIsReclaimed(t *testing.T) {
	failing := true
	attempts := 0
	srv, rdb, mr := newEventTestServer(t, &MockJobRepository{
		UpdateJobStatusFunc: func(ctx context.Context, arg db.UpdateJobStatusParams) error {
			attempts++
			if failing {
				return fmt.Errorf("db down")
			}
			return nil
		},
	})
	addEvent(t, rdb, map[string]interface{}{"job_id": "j1", "status": "success", "output_path": "outputs/j1.pdf"})

	messages, _ := srv.nextJobEvents()
	srv.handleJobEvent(messages[0])
	if n := pendingCount(t, rdb); n != 1 {
		t.Fatalf("event must stay pending after a DB error, pending=%d", n)
	}

	// Not idle long enough yet: nothing is handed out again.
	if again, _ := srv.nextJobEvents(); len(again) != 0 {
		t.Fatalf("event must not be redelivered immediately, got %d", len(again))
	}

	// Let it idle past the claim threshold. miniredis uses a fake clock for stream idle times.
	failing = false
	mr.SetTime(time.Now().Add(2 * jobEventClaimIdle))
	reclaimed, err := srv.nextJobEvents()
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("expected the event to be reclaimed, got %v, %v", reclaimed, err)
	}
	srv.handleJobEvent(reclaimed[0])
	if n := pendingCount(t, rdb); n != 0 || attempts != 2 {
		t.Errorf("after retry pending=%d attempts=%d", n, attempts)
	}
}

func TestHandleJobEvent_WebSocketWriteFailureStillAcks(t *testing.T) {
	srv, rdb, _ := newEventTestServer(t, &MockJobRepository{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		srv.hub.Register("j1", conn)
	}))
	defer server.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	// Close the server side too, so the write fails.
	time.Sleep(50 * time.Millisecond)
	srv.hub.mux.RLock()
	conn := srv.hub.connections["j1"]
	srv.hub.mux.RUnlock()
	conn.Close()

	addEvent(t, rdb, map[string]interface{}{"job_id": "j1", "status": "success", "output_path": "outputs/j1.pdf"})
	messages, _ := srv.nextJobEvents()
	srv.handleJobEvent(messages[0])

	if n := pendingCount(t, rdb); n != 0 {
		t.Errorf("event must be acknowledged even if the push failed, pending=%d", n)
	}
	srv.hub.mux.RLock()
	_, stillThere := srv.hub.connections["j1"]
	srv.hub.mux.RUnlock()
	if stillThere {
		t.Error("dead connection must be removed from the hub")
	}
}
