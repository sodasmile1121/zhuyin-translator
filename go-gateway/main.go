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
	"strings"
	"sync"
	"time"

	"go-gateway/db"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type JobRepository interface {
	CreateJob(ctx context.Context, arg db.CreateJobParams) error
	UpdateJobStatus(ctx context.Context, arg db.UpdateJobStatusParams) error
	GetJobStatus(ctx context.Context, jobID string) (string, error)
}

type StorageService interface {
	UploadFile(ctx context.Context, file multipart.File, s3Key string) error
	GeneratePresignedURL(ctx context.Context, s3Key string, expires time.Duration) (string, error)
}

type QueueService interface {
	PublishTask(ctx context.Context, jobID, filePath string) error
	FetchResultCache(ctx context.Context, jobID string) ([]byte, error)
}

type SQLJobRepository struct {
	queries *db.Queries
}

func (r *SQLJobRepository) CreateJob(ctx context.Context, arg db.CreateJobParams) error {
	return r.queries.CreateJob(ctx, arg)
}

func (r *SQLJobRepository) UpdateJobStatus(ctx context.Context, arg db.UpdateJobStatusParams) error {
	return r.queries.UpdateJobStatus(ctx, arg)
}

func (r *SQLJobRepository) GetJobStatus(ctx context.Context, jobID string) (string, error) {
	return r.queries.GetJobStatus(ctx, jobID)
}

type S3StorageService struct {
	client     *s3.Client
	bucketName string
}

func (s *S3StorageService) UploadFile(ctx context.Context, file multipart.File, s3Key string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s3Key),
		Body:   file,
	})
	return err
}

func (s *S3StorageService) GeneratePresignedURL(ctx context.Context, s3Key string, expires time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(s.client)
	presignedReq, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s3Key),
	}, s3.WithPresignExpires(expires))
	if err != nil {
		return "", err
	}
	return presignedReq.URL, nil
}

type RedisQueueService struct {
	rdb *redis.Client
}

func (q *RedisQueueService) PublishTask(ctx context.Context, jobID, filePath string) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: TaskStreamKey,
		ID:     "*",
		Values: map[string]interface{}{
			"job_id":    jobID,
			"file_path": filePath,
		},
	}).Err()
}

func (q *RedisQueueService) FetchResultCache(ctx context.Context, jobID string) ([]byte, error) {
	redisKey := fmt.Sprintf("result:%s", jobID)
	compressedBytes, err := q.rdb.Get(ctx, redisKey).Bytes()
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

type Server struct {
	id      string
	repo    JobRepository
	storage StorageService
	queue   QueueService
	rdb     *redis.Client
	db      *sql.DB
	ctx     context.Context
	hub     *Hub
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

const TaskStreamKey = "stream:task"
const StreamKey = "stream:job"
const GroupName = "go-gateway-group"

func NewHub() *Hub {
	return &Hub{connections: make(map[string]*websocket.Conn)}
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
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			log.Println("Redis Stream group warning:", err)
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
				log.Println("Failed to read from stream:", err)
			}
			for _, stream := range streams {
				for _, message := range stream.Messages {
					jobID, ok := message.Values["job_id"].(string)
					if !ok {
						continue
					}
					status, ok := message.Values["status"].(string)
					if !ok {
						continue
					}
					outputPath, ok := message.Values["output_path"].(string)
					if !ok {
						continue
					}
					payload, err := json.Marshal(message.Values)
					if err != nil {
						log.Println("Fail to marshal json:", err)
						continue
					}

					err = s.repo.UpdateJobStatus(s.ctx, db.UpdateJobStatusParams{
						Status:      status,
						S3OutputUrl: sql.NullString{String: outputPath, Valid: outputPath != ""},
						JobID:       jobID,
					})
					if err != nil {
						log.Println("Database UPDATE error:", err)
						continue
					}

					s.hub.mux.RLock()
					conn, exists := s.hub.connections[jobID]
					s.hub.mux.RUnlock()
					if exists {
						err := conn.WriteMessage(websocket.TextMessage, []byte(payload))
						if err != nil {
							log.Printf("Failed to push notification to Job %s: %v\n", jobID, err)
						} else {
							s.rdb.XAck(s.ctx, StreamKey, GroupName, message.ID)
							s.hub.Unregister(jobID)
							log.Printf("Successfully pushed job %s completion message to frontend!\n", jobID)
						}
					} else {
						s.rdb.XAck(s.ctx, StreamKey, GroupName, message.ID)
						log.Printf("The websocket for job %s has not been established\n", jobID)
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
		log.Println("Fail to upgrade WebSocket:", err)
		return
	}
	log.Printf("Frontend connected successfully. Job %s is listening...\n", jobID)
	s.hub.Register(jobID, conn)

	go func() {
		result, err := s.queue.FetchResultCache(s.ctx, jobID)
		if err == nil && len(result) > 0 {
			err = conn.WriteMessage(websocket.TextMessage, result)
			if err == nil {
				log.Printf("Job %s finished during downtime. Notification auto-backfilled.\n", jobID)
				conn.Close()
			}
			return
		}
	}()

	go func() {
		defer func() {
			s.hub.Unregister(jobID)
			log.Printf("Job %s is disconnected from Go\n", jobID)
		}()
		for {
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
			http.Error(w, fmt.Sprintf("%s is not a pdf file", fh.Filename), http.StatusBadRequest)
			return
		}
	}

	var assignedJobs []JobInfo
	for _, fh := range files {
		jobID := uuid.New().String()
		s3Key := "inputs/" + jobID + "_" + fh.Filename

		srcFile, err := fh.Open()
		if err != nil {
			http.Error(w, "Failed to open file", http.StatusInternalServerError)
			return
		}

		err = s.storage.UploadFile(s.ctx, srcFile, s3Key)
		srcFile.Close()
		if err != nil {
			http.Error(w, "Failed to upload to S3", http.StatusInternalServerError)
			return
		}

		presignedURL, err := s.storage.GeneratePresignedURL(s.ctx, s3Key, 15*time.Minute)
		if err != nil {
			http.Error(w, "Failed to generate presigned URL", http.StatusInternalServerError)
			return
		}

		err = s.repo.CreateJob(s.ctx, db.CreateJobParams{
			JobID:      jobID,
			Status:     "pending",
			S3InputUrl: presignedURL,
		})
		if err != nil {
			http.Error(w, "Failed to save job to DB", http.StatusInternalServerError)
			return
		}

		err = s.queue.PublishTask(s.ctx, jobID, presignedURL)
		if err != nil {
			http.Error(w, "Failed to publish task to queue", http.StatusInternalServerError)
			return
		}

		assignedJobs = append(assignedJobs, JobInfo{
			JobID:    jobID,
			FilePath: presignedURL,
		})
	}

	w.Header().Set("Content-Type", "application/json")
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

	resultData, err := s.queue.FetchResultCache(s.ctx, jobID)
	if err == nil {
		w.WriteHeader(http.StatusOK)
		w.Write(resultData)
		return
	}

	if err == redis.Nil {
		status, err := s.repo.GetJobStatus(s.ctx, jobID)
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

	resultData, err := s.queue.FetchResultCache(s.ctx, jobID)
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
		http.Error(w, fmt.Sprintf("Cannot download. Job failed: %s", payload.Error), http.StatusUnprocessableEntity)
		return
	}

	downloadURL, err := s.storage.GeneratePresignedURL(s.ctx, payload.PdfPath, 10*time.Minute)
	if err != nil {
		http.Error(w, "Failed to generate download URL", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, downloadURL, http.StatusFound)
}

func main() {
	region := os.Getenv("AWS_REGION")
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	bucketName := os.Getenv("AWS_S3_BUCKET")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.Fatalf("Cannot initialize AWS config: %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	log.Println("AWS S3 has successfully initialized")

	redisAddr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	pgDSN := os.Getenv("DATABASE_URL")
	dbConn, err := sql.Open("pgx", pgDSN)
	if err != nil {
		log.Fatalf("Fail to initiate Postgres: %v", err)
	}

	dbConn.SetMaxOpenConns(100)
	dbConn.SetMaxIdleConns(50)

	if err := dbConn.Ping(); err != nil {
		log.Fatalf("Fail to connect to Postgres: %v", err)
	}

	ctx := context.Background()
	hub := NewHub()

	queries := db.New(dbConn)
	repo := &SQLJobRepository{queries: queries}

	storage := &S3StorageService{client: s3Client, bucketName: bucketName}
	queue := &RedisQueueService{rdb: rdb}

	srv := &Server{
		id:      "srv1",
		repo:    repo,
		storage: storage,
		queue:   queue,
		rdb:     rdb,
		db:      dbConn,
		ctx:     ctx,
		hub:     hub,
	}

	srv.startRedisSub()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./index.html")
	})
	http.HandleFunc("/upload", srv.handleUpload)
	http.HandleFunc("/status", srv.handleStatus)
	http.HandleFunc("/ws", srv.handleWebSocket)
	http.HandleFunc("/download", srv.handleDownload)

	log.Println("Server is running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
