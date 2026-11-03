package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed",
		},
		[]string{"path", "status"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds",
			Buckets: prometheus.DefBuckets, // 預設 0.005s ~ 10s 的區間
		},
		[]string{"path"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

var corsGuard = struct {
	sync.RWMutex
	allowedOrigins map[string]bool
}{
	allowedOrigins: make(map[string]bool),
}

type JobRepository interface {
	CreateJob(ctx context.Context, arg db.CreateJobParams) error
	UpdateJobStatus(ctx context.Context, arg db.UpdateJobStatusParams) error
	GetJob(ctx context.Context, jobID string) (db.Job, error)
	ListStuckJobs(ctx context.Context, olderThan time.Duration, limit int32) ([]db.ListStuckJobsRow, error)
}

type StorageService interface {
	UploadFile(ctx context.Context, file multipart.File, s3Key string) error
	GeneratePresignedURL(ctx context.Context, s3Key string, expires time.Duration) (string, error)
}

type QueueService interface {
	PublishTask(ctx context.Context, jobID, filePath string) error
	FetchResultCache(ctx context.Context, jobID string) ([]byte, error)
	// TryMarkRequeued reports whether the caller is the first to mark jobID within ttl.
	TryMarkRequeued(ctx context.Context, jobID string, ttl time.Duration) (bool, error)
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

func (r *SQLJobRepository) GetJob(ctx context.Context, jobID string) (db.Job, error) {
	return r.queries.GetJob(ctx, jobID)
}

func (r *SQLJobRepository) ListStuckJobs(ctx context.Context, olderThan time.Duration, limit int32) ([]db.ListStuckJobsRow, error) {
	return r.queries.ListStuckJobs(ctx, db.ListStuckJobsParams{
		OlderThanSecs: olderThan.Seconds(),
		MaxRows:       limit,
	})
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

func (q *RedisQueueService) TryMarkRequeued(ctx context.Context, jobID string, ttl time.Duration) (bool, error) {
	return q.rdb.SetNX(ctx, "requeued:"+jobID, "1", ttl).Result()
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

type statusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	PdfPath string `json:"pdf_path,omitempty"`
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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		corsGuard.RLock()
		allowed := corsGuard.allowedOrigins[origin]
		corsGuard.RUnlock()

		if allowed {
			return true
		}
		log.Printf("WebSocket Rejected Origin: %s\n", origin)
		return false
	},
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		origin := r.Header.Get("Origin")
		corsGuard.RLock()
		allowed := corsGuard.allowedOrigins[origin]
		corsGuard.RUnlock()

		if allowed && origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		} else {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
		return
	}
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
	statusCode := "200"
	defer func() {
		httpRequestsTotal.WithLabelValues("/upload", statusCode).Inc()
	}()

	if !s.setCORS(w, r) {
		statusCode = "403"
		return
	}
	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		statusCode = "400"
		http.Error(w, "Failed to parse data", http.StatusBadRequest)
		return
	}

	files := r.MultipartForm.File["files"]
	for _, fh := range files {
		if fh.Header.Get("Content-Type") != "application/pdf" {
			statusCode = "400"
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
			statusCode = "500"
			http.Error(w, "Failed to open file", http.StatusInternalServerError)
			return
		}

		err = s.storage.UploadFile(s.ctx, srcFile, s3Key)
		srcFile.Close()
		if err != nil {
			statusCode = "500"
			http.Error(w, "Failed to upload to S3", http.StatusInternalServerError)
			return
		}

		presignedURL, err := s.storage.GeneratePresignedURL(s.ctx, s3Key, 15*time.Minute)
		if err != nil {
			statusCode = "500"
			http.Error(w, "Failed to generate presigned URL", http.StatusInternalServerError)
			return
		}

		err = s.repo.CreateJob(s.ctx, db.CreateJobParams{
			JobID:      jobID,
			Status:     "pending",
			S3InputUrl: presignedURL,
		})
		if err != nil {
			statusCode = "500"
			http.Error(w, "Failed to save job to DB", http.StatusInternalServerError)
			return
		}

		err = s.queue.PublishTask(s.ctx, jobID, presignedURL)
		if err != nil {
			statusCode = "500"
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
	if _, err := w.Write(jsonResponse); err != nil {
		log.Println("Failed to write HTTP response:", err)
	}
}

// writeJSON sends a JSON body with the right Content-Type. http.Error cannot be used for JSON because it
// overwrites Content-Type with text/plain.
func writeJSON(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(body); err != nil {
		log.Println("Failed to write HTTP response:", err)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !s.setCORS(w, r) {
		return
	}
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, []byte(`{"error": "Missing job_id"}`))
		return
	}

	resultData, err := s.queue.FetchResultCache(s.ctx, jobID)
	if err == nil {
		writeJSON(w, http.StatusOK, resultData)
		return
	}
	// A missing entry is normal (it expires after an hour). Any other cache error (Redis down, corrupt
	// entry) must not hide what Postgres knows, so both fall back to the database.
	if !errors.Is(err, redis.Nil) {
		log.Printf("Result cache unavailable for job %s, falling back to the database: %v\n", jobID, err)
	}

	job, err := s.repo.GetJob(s.ctx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, []byte(`{"error": "Job not found"}`))
		return
	} else if err != nil {
		writeJSON(w, http.StatusInternalServerError, []byte(`{"error": "Database error"}`))
		return
	}

	body, _ := json.Marshal(statusResponse{
		Status:  job.Status,
		Message: "Job is currently " + job.Status,
		PdfPath: job.S3OutputUrl.String,
	})
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if !s.setCORS(w, r) {
		return
	}
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, []byte(`{"error": "Missing job_id"}`))
		return
	}

	// The cache entry expires after an hour, and it may also be unreadable (Redis down, corrupt entry).
	// In every such case Postgres still knows how the job ended.
	resultData, err := s.queue.FetchResultCache(s.ctx, jobID)
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("Result cache unavailable for job %s, falling back to the database: %v\n", jobID, err)
		}
		s.downloadFromDB(w, r, jobID)
		return
	}

	var payload ResultPayload
	if err := json.Unmarshal(resultData, &payload); err != nil {
		log.Printf("Corrupt cache entry for job %s, falling back to the database: %v\n", jobID, err)
		s.downloadFromDB(w, r, jobID)
		return
	}

	if payload.Status == "failed" || payload.PdfPath == "" {
		http.Error(w, fmt.Sprintf("Cannot download. Job failed: %s", payload.Error), http.StatusUnprocessableEntity)
		return
	}

	s.redirectToOutput(w, r, payload.PdfPath)
}

func (s *Server) redirectToOutput(w http.ResponseWriter, r *http.Request, s3Key string) {
	downloadURL, err := s.storage.GeneratePresignedURL(s.ctx, s3Key, 10*time.Minute)
	if err != nil {
		http.Error(w, "Failed to generate download URL", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, downloadURL, http.StatusFound)
}

func (s *Server) downloadFromDB(w http.ResponseWriter, r *http.Request, jobID string) {
	job, err := s.repo.GetJob(s.ctx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	switch job.Status {
	case "success":
		if !job.S3OutputUrl.Valid || job.S3OutputUrl.String == "" {
			http.Error(w, "Job finished but has no output file", http.StatusInternalServerError)
			return
		}
		s.redirectToOutput(w, r, job.S3OutputUrl.String)
	case "failed":
		http.Error(w, "Cannot download. Job failed.", http.StatusUnprocessableEntity)
	default:
		http.Error(w, "File is still processing, please wait...", http.StatusNotFound)
	}
}

func setUpOrigins() {
	originsEnv := os.Getenv("ALLOWED_ORIGINS")
	corsGuard.Lock()
	defer corsGuard.Unlock()
	if originsEnv == "" {
		corsGuard.allowedOrigins["http://localhost:8081"] = true
		log.Println("CORS: No ALLOWED_ORIGINS env found, fallback to localhost:8081")
	} else {
		parts := strings.Split(originsEnv, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				corsGuard.allowedOrigins[trimmed] = true
				log.Printf("CORS: Allowed origin registered -> %s\n", trimmed)
			}
		}
	}
}

func (s *Server) setCORS(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" {
		corsGuard.RLock()
		allowed := corsGuard.allowedOrigins[origin]
		corsGuard.RUnlock()

		if !allowed {
			log.Printf("CORS Rejected Origin: %s\n", origin)
			http.Error(w, "CORS Not Allowed", http.StatusForbidden)
			return false
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
	} else {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}

	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return false
	}
	return true
}

func promMiddleware(path string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		handler(w, r)

		duration := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(path).Observe(duration)
	}
}

func main() {
	setUpOrigins()
	region := os.Getenv("AWS_REGION")
	bucketName := os.Getenv("AWS_S3_BUCKET")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
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
	var dbConn *sql.DB
	for i := 0; i < 6; i++ {
		dbConn, err = sql.Open("pgx", pgDSN)
		if err == nil {
			err = dbConn.Ping()
		}
		if err == nil {
			log.Println("Postgres connected successfully!")
			break
		}
		log.Printf("Postgres is not ready yet (error: %v), retrying in 3s... (%d/6)\n", err, i+1)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		log.Fatalf("Fail to connect to Postgres after multiple retries: %v", err)
	}

	dbConn.SetMaxOpenConns(100)
	dbConn.SetMaxIdleConns(50)

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
	go newReconcilerFromEnv(repo, storage, queue).Run(ctx)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./index.html")
	})
	http.HandleFunc("/upload", promMiddleware("/upload", srv.handleUpload))
	http.HandleFunc("/status", promMiddleware("/status", srv.handleStatus))
	http.HandleFunc("/ws", srv.handleWebSocket)
	http.HandleFunc("/download", promMiddleware("/download", srv.handleDownload))
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))
}
