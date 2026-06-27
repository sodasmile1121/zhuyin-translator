package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	rdb *redis.Client
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
	pubsub := s.rdb.Subscribe(s.ctx, "job_status_channel")
	ch := pubsub.Channel()

	go func() {
		for msg := range ch {
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				continue
			}
			jobID, ok := data["job_id"].(string)
			if !ok {
				continue
			}
			s.hub.mux.RLock()
			conn, exists := s.hub.connections[jobID]
			s.hub.mux.RUnlock()

			if exists {
				err := conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
				if err != nil {
					fmt.Printf("Failed to push notification to Job %s: %v\n", jobID, err)
				} else {
					fmt.Printf("Successfully pushed job %s completion message to frontend!\n", jobID)
				}
				s.hub.Unregister(jobID)
			} else {
				fmt.Printf("The websocket for job %s has not been established", jobID)
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
		fmt.Println("Fail to upgrade WebSocket:", err)
		return
	}
	fmt.Printf("Frontend connnect Successfully to Go. Job %s is listening...\n", jobID)
	s.hub.Register(jobID, conn)
	go func() {
		defer func() {
			s.hub.Unregister(jobID)
			fmt.Printf("Job %s is disconnected to Go。\n", jobID)
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
	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		http.Error(w, "Failed to parse data", http.StatusBadRequest)
		return
	}
	files := r.MultipartForm.File["files"]
	for _, fh := range files {
		if fh.Header.Get("Content-Type") != "application/pdf" {
			msg := fmt.Sprintf("%s is not a pdf file", fh.Filename)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
	}
	paths, err := saveFiles(files)
	if err != nil {
		http.Error(w, "Failed to save files to disk", http.StatusInternalServerError)
		return
	}

	key := "queue:job"
	var assignedJobs []JobInfo

	for _, path := range paths {
		job := JobInfo{
			JobID:    uuid.New().String(),
			FilePath: path,
		}
		jsonBytes, err := json.Marshal(job)
		if err != nil {
			http.Error(w, "Failed to create JSON payload", http.StatusInternalServerError)
			return
		}
		err = s.rdb.LPush(s.ctx, key, jsonBytes).Err()
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

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, `{"error": "Missing job_id"}`, http.StatusBadRequest)
		return
	}
	redisKey := fmt.Sprintf("result:%s", jobID)
	resultData, err := s.rdb.Get(s.ctx, redisKey).Result()
	if err == redis.Nil {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "processing", "message": "AI is still compiling..."}`))
		return
	} else if err != nil {
		http.Error(w, `{"error": "Redis fetch error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(resultData))
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	},
	)
	ctx := context.Background()
	hub := NewHub()
	srv := &Server{rdb: rdb, ctx: ctx, hub: hub}
	srv.startRedisSub()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./index.html")
	})
	http.HandleFunc("/upload", srv.handleUpload)
	http.HandleFunc("/status", srv.handleStatus)
	http.HandleFunc("/ws", srv.handleWebSocket)
	fmt.Println("Server is running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
