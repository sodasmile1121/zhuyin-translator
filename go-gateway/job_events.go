package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"go-gateway/db"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	// An event that stayed unacknowledged this long is handed out again (by this consumer, after a failure or restart).
	jobEventClaimIdle = 30 * time.Second
	jobEventBlock     = time.Second
	jobEventBackoff   = time.Second
)

func (s *Server) startRedisSub() {
	go s.consumeJobEvents()
}

func (s *Server) consumeJobEvents() {
	err := s.rdb.XGroupCreateMkStream(s.ctx, StreamKey, GroupName, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Println("Redis Stream group warning:", err)
	}
	for {
		if s.ctx.Err() != nil {
			return
		}
		messages, err := s.nextJobEvents()
		if err != nil {
			if errors.Is(err, redis.ErrClosed) || errors.Is(err, context.Canceled) {
				log.Println("Redis client closed, exiting subscription loop gracefully.")
				return
			}
			log.Println("Failed to read from stream:", err)
			select {
			case <-time.After(jobEventBackoff):
			case <-s.ctx.Done():
				return
			}
			continue
		}
		for _, message := range messages {
			s.handleJobEvent(message)
		}
	}
}

// nextJobEvents returns events that were delivered earlier but never acknowledged (XAUTOCLAIM) first,
// and only then new ones, so an event is not lost when the gateway crashes or the database is briefly down.
func (s *Server) nextJobEvents() ([]redis.XMessage, error) {
	claimed, _, err := s.rdb.XAutoClaim(s.ctx, &redis.XAutoClaimArgs{
		Stream:   StreamKey,
		Group:    GroupName,
		Consumer: s.id,
		MinIdle:  jobEventClaimIdle,
		Start:    "0-0",
		Count:    10,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(claimed) > 0 {
		return claimed, nil
	}

	streams, err := s.rdb.XReadGroup(s.ctx, &redis.XReadGroupArgs{
		Group:    GroupName,
		Consumer: s.id,
		Streams:  []string{StreamKey, ">"},
		Count:    10,
		Block:    jobEventBlock,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var messages []redis.XMessage
	for _, stream := range streams {
		messages = append(messages, stream.Messages...)
	}
	return messages, nil
}

func parseJobEvent(values map[string]interface{}) (jobID, status, outputPath string, ok bool) {
	jobID, _ = values["job_id"].(string)
	status, _ = values["status"].(string)
	outputPath, _ = values["output_path"].(string)
	return jobID, status, outputPath, jobID != "" && status != ""
}

// handleJobEvent stores the final status, notifies the browser and acknowledges the event.
// Only a database failure leaves the event unacknowledged: retrying is the one thing that can help there.
func (s *Server) handleJobEvent(message redis.XMessage) {
	jobID, status, outputPath, ok := parseJobEvent(message.Values)
	if !ok {
		log.Printf("Dropping malformed job event %s: %v\n", message.ID, message.Values)
		s.ackJobEvent(message.ID)
		return
	}

	err := s.repo.UpdateJobStatus(s.ctx, db.UpdateJobStatusParams{
		Status:      status,
		S3OutputUrl: sql.NullString{String: outputPath, Valid: outputPath != ""},
		JobID:       jobID,
	})
	if err != nil {
		log.Printf("Database UPDATE error for job %s, event %s stays pending: %v\n", jobID, message.ID, err)
		return
	}

	s.hub.mux.RLock()
	conn, exists := s.hub.connections[jobID]
	s.hub.mux.RUnlock()
	if !exists {
		log.Printf("The websocket for job %s has not been established\n", jobID)
		s.ackJobEvent(message.ID)
		return
	}

	payload, err := json.Marshal(message.Values)
	if err == nil {
		err = conn.WriteMessage(websocket.TextMessage, payload)
	}
	if err != nil {
		// The result is already in Redis and Postgres, so a client that reconnects (or polls) still gets it.
		log.Printf("Failed to push notification to job %s: %v\n", jobID, err)
	} else {
		log.Printf("Successfully pushed job %s completion message to frontend!\n", jobID)
	}
	s.hub.Unregister(jobID)
	s.ackJobEvent(message.ID)
}

func (s *Server) ackJobEvent(messageID string) {
	if err := s.rdb.XAck(s.ctx, StreamKey, GroupName, messageID).Err(); err != nil {
		log.Printf("Failed to ACK job event %s: %v\n", messageID, err)
	}
}
