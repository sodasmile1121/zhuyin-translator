package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	id  string
	rdb *redis.Client
	db  *sql.DB
	ctx context.Context
	hub *Hub
}

type JobInfo struct {
	JobID    string `json:"job_id"`
	FilePath string `json:"file_path"`
}

type Hub struct {
	connections map[string]*websocket.Conn
	mux         sync.RWMutex
}

type ResultPayload struct {
	Status  string `json:"status"`
	PdfPath string `json:"pdf_path"`
	Error   string `json:"error"`
}

const QueueKey = "queue:job"
const StreamKey = "stream:job"
const GroupName = "go-gateway-group"

func saveFiles(files []*multipart.FileHeader) ([]string, error) {
	dir := "./uploads"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var savePaths []string
	success := false

	defer func() {
		if !success {
			for _, path := range savePaths {
				os.Remove(path)
			}
		}
	}()

	for _, fh := range files {
		srcFile, err := fh.Open()
		if err != nil {
			return nil, err
		}

		newFileName := uuid.New().String() + "_" + fh.Filename
		finalPath := filepath.Join(dir, newFileName)
		dstFile, err := os.Create(finalPath)
		if err != nil {
			srcFile.Close()
			return nil, err
		}

		_, err = io.Copy(dstFile, srcFile)
		if err != nil {
			srcFile.Close()
			dstFile.Close()
			return nil, err
		}
		srcFile.Close()
		dstFile.Close()
		absPath, err := filepath.Abs(finalPath)
		if err != nil {
			return nil, err
		}
		savePaths = append(savePaths, absPath)
	}
	success = true
	return savePaths, nil
}

func NewHub() *Hub {
	return &Hub{
		connections: make(map[string]*websocket.Conn),
	}
}

func (h *Hub) Register(jobID string, conn *websocket.Conn) {
	h.mux.Lock()
	defer h.mux.Unlock()
	h.connections[jobID] = conn
}

func (h *Hub) Unregister(jobID string) {
	h.mux.Lock()
	defer h.mux.Unlock()
	if conn, exists := h.connections[jobID]; exists {
		conn.Close()
		delete(h.connections, jobID)
	}
}

func (s *Server) startRedisSub() {
	go func() {
		err := s.rdb.XGroupCreateMkStream(s.ctx, StreamKey, GroupName, "$").Err()
		if err != nil {
			fmt.Println("Redis error:", err)
		}
		for {
			streams, err := s.rdb.XReadGroup(s.ctx, &redis.XReadGroupArgs{
				Group:    GroupName,
				Consumer: s.id,
				Streams:  []string{StreamKey, ">"},
				Count:    1,
				Block:    100 * time.Millisecond,
			}).Result()
			if err != nil {
				if err == redis.Nil {
					continue
				}
				fmt.Println("Failed to read from stream:", err)
			}
			for _, stream := range streams {
				for _, message := range stream.Messages {
					jobID, ok := message.Values["job_id"].(string)
					if !ok {
						continue
					}
					payload, err := json.Marshal(message.Values)
					if err != nil {
						fmt.Println("Fail to marshal json:", err)
						continue
					}
					s.hub.mux.RLock()
					conn, exists := s.hub.connections[jobID]
					s.hub.mux.RUnlock()
					if exists {
						err := conn.WriteMessage(websocket.TextMessage, []byte(payload))
						if err != nil {
							fmt.Printf("Failed to push notification to Job %s: %v\n", jobID, err)
						} else {
							s.rdb.XAck(s.ctx, StreamKey, GroupName, message.ID)
							s.hub.Unregister(jobID)
							fmt.Printf("Successfully pushed job %s completion message to frontend!\n", jobID)
						}
					} else {
						s.rdb.XAck(s.ctx, StreamKey, GroupName, message.ID)
						fmt.Printf("The websocket for job %s has not been established\n", jobID)
					}
				}
			}
		}
	}()
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, "Missing job_id query parameter", http.StatusBadRequest)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Fail to upgrade WebSocket:\n", err)
		return
	}
	fmt.Printf("Frontend connnect Successfully to Go. Job %s is listening...\n", jobID)
	s.hub.Register(jobID, conn)

	go func() {
		result, err := s.fetchJobResult(jobID)
		if err == nil && len(result) > 0 {
			err = conn.WriteMessage(websocket.TextMessage, result)
			if err == nil {
				fmt.Printf("Job %s finished during downtime. Notification auto-backfilled.", jobID)
				conn.Close()
			}
			return
		}
	}()

	go func() {
		defer func() {
			s.hub.Unregister(jobID)
			fmt.Printf("Job %s is disconnected to Go\n", jobID)
		}()
		for {
			// If frontend close or disconnect, ReadMessage report error, triggering break
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}()
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		http.Error(w, "Failed to parse data", http.StatusBadRequest)
		return
	}
	files := r.MultipartForm.File["files"]
	for _, fh := range files {
		if fh.Header.Get("Content-Type") != "application/pdf" {
			msg := fmt.Sprintf("%s is not a pdf file\n", fh.Filename)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
	}
	paths, err := saveFiles(files)
	if err != nil {
		http.Error(w, "Failed to save files to disk", http.StatusInternalServerError)
		return
	}

	var assignedJobs []JobInfo

	for _, path := range paths {
		job := JobInfo{
			JobID:    uuid.New().String(),
			FilePath: path,
		}
		query := `INSERT INTO jobs (job_id, status, s3_input_url) VALUES ($1, $2, $3);`
		_, err = s.db.Exec(query, job.JobID, "pending", job.FilePath)
		if err != nil {
			fmt.Println("Postgres INSERT error:", err)
			http.Error(w, "Failed to save job to database", http.StatusInternalServerError)
			return
		}
		jsonBytes, err := json.Marshal(job)
		if err != nil {
			http.Error(w, "Failed to create JSON payload", http.StatusInternalServerError)
			return
		}
		err = s.rdb.LPush(s.ctx, QueueKey, jsonBytes).Err()
		if err != nil {
			http.Error(w, "Failed to push tasks to Redis queue", http.StatusInternalServerError)
			return
		}
		assignedJobs = append(assignedJobs, job)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	jsonResponse, _ := json.Marshal(assignedJobs)
	w.Write(jsonResponse)
}

func (s *Server) fetchJobResult(jobID string) ([]byte, error) {
	redisKey := fmt.Sprintf("result:%s", jobID)
	compressedBytes, err := s.rdb.Get(s.ctx, redisKey).Bytes()
	if err != nil {
		return nil, err
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressedBytes))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, `{"error": "Missing job_id"}`, http.StatusBadRequest)
		return
	}
	resultData, err := s.fetchJobResult(jobID)
	if err == nil {
		w.WriteHeader(http.StatusOK)
		w.Write(resultData)
		return
	}
	if err == redis.Nil {
		var status string
		query := "SELECT status FROM jobs WHERE job_id = $1;"
		err := s.db.QueryRow(query, jobID).Scan(&status)

		if err == sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error": "Job not found"}`))
			return
		} else if err != nil {
			http.Error(w, `{"error": "Database error"}`, http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "%s", "message": "Job is currently %s"}`, status, status)
		return
	}
	http.Error(w, `{"error": "Fetch error"}`, http.StatusInternalServerError)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, `{"error": "Missing job_id"}`, http.StatusBadRequest)
		return
	}
	resultData, err := s.fetchJobResult(jobID)
	if err == redis.Nil {
		http.Error(w, "File is still processing, please wait...", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Read decompress data error", http.StatusInternalServerError)
		return
	}
	var payload ResultPayload
	if err := json.Unmarshal(resultData, &payload); err != nil {
		http.Error(w, "Parse JSON error", http.StatusInternalServerError)
		return
	}
	if payload.Status == "failed" || payload.PdfPath == "" {
		msg := fmt.Sprintf("Cannot download. Job failed: %s", payload.Error)
		http.Error(w, msg, http.StatusUnprocessableEntity)
		return
	}
	physicalPath := filepath.Join("/app", payload.PdfPath)
	if _, err := os.Stat(physicalPath); os.IsNotExist(err) {
		http.Error(w, "Physical PDF file not found on disk", http.StatusNotFound)
		return
	}

	fileName := filepath.Base(physicalPath)
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))
	http.ServeFile(w, r, physicalPath)
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	},
	)

	pgDSN := os.Getenv("DATABASE_URL")
	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		log.Fatalf("Fail to initiate Postgres: %v", err)
	}

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)

	if err := db.Ping(); err != nil {
		log.Fatalf("Fail to connect to Postgres: %v", err)
	}
	ctx := context.Background()
	hub := NewHub()
	srv := &Server{id: "srv1", rdb: rdb, db: db, ctx: ctx, hub: hub}
	srv.startRedisSub()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./index.html")
	})
	http.HandleFunc("/upload", srv.handleUpload)
	http.HandleFunc("/status", srv.handleStatus)
	http.HandleFunc("/ws", srv.handleWebSocket)
	http.HandleFunc("/download", srv.handleDownload)
	fmt.Println("Server is running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
