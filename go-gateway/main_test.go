package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"go-gateway/db"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type MockJobRepository struct {
	CreateJobFunc       func(ctx context.Context, arg db.CreateJobParams) error
	UpdateJobStatusFunc func(ctx context.Context, arg db.UpdateJobStatusParams) error
	GetJobStatusFunc    func(ctx context.Context, jobID string) (string, error)
}

func (m *MockJobRepository) CreateJob(ctx context.Context, arg db.CreateJobParams) error {
	if m.CreateJobFunc != nil {
		return m.CreateJobFunc(ctx, arg)
	}
	return nil
}
func (m *MockJobRepository) UpdateJobStatus(ctx context.Context, arg db.UpdateJobStatusParams) error {
	if m.UpdateJobStatusFunc != nil {
		return m.UpdateJobStatusFunc(ctx, arg)
	}
	return nil
}
func (m *MockJobRepository) GetJobStatus(ctx context.Context, jobID string) (string, error) {
	if m.GetJobStatusFunc != nil {
		return m.GetJobStatusFunc(ctx, jobID)
	}
	return "pending", nil
}

type MockStorageService struct {
	UploadFileFunc           func(ctx context.Context, file multipart.File, s3Key string) error
	GeneratePresignedURLFunc func(ctx context.Context, s3Key string, expires time.Duration) (string, error)
}

func (m *MockStorageService) UploadFile(ctx context.Context, file multipart.File, s3Key string) error {
	if m.UploadFileFunc != nil {
		return m.UploadFileFunc(ctx, file, s3Key)
	}
	return nil
}

func (m *MockStorageService) GeneratePresignedURL(ctx context.Context, s3Key string, expires time.Duration) (string, error) {
	if m.GeneratePresignedURLFunc != nil {
		return m.GeneratePresignedURLFunc(ctx, s3Key, expires)
	}
	return "https://mock-s3.com/" + s3Key, nil
}

type MockQueueService struct {
	PublishTaskFunc      func(ctx context.Context, jobID, filePath string) error
	FetchResultCacheFunc func(ctx context.Context, jobID string) ([]byte, error)
}

func (m *MockQueueService) PublishTask(ctx context.Context, jobID, filePath string) error {
	if m.PublishTaskFunc != nil {
		return m.PublishTaskFunc(ctx, jobID, filePath)
	}
	return nil
}
func (m *MockQueueService) FetchResultCache(ctx context.Context, jobID string) ([]byte, error) {
	if m.FetchResultCacheFunc != nil {
		return m.FetchResultCacheFunc(ctx, jobID)
	}
	return nil, redis.Nil
}

func TestHandleUpload_AllBranches(t *testing.T) {
	cases := []struct {
		name          string
		storage       *MockStorageService
		repo          *MockJobRepository
		queue         *MockQueueService
		fieldName     string
		filename      string
		contentType   string
		content       []byte
		expectCode    int
		expectBodyHas string
	}{
		{
			name:        "success upload pdf",
			storage:     &MockStorageService{},
			repo:        &MockJobRepository{},
			queue:       &MockQueueService{},
			fieldName:   "files",
			filename:    "test.pdf",
			contentType: "application/pdf",
			content:     []byte("%PDF-1.4"),
			expectCode:  http.StatusOK,
		},
		{
			name:        "invalid file type",
			storage:     &MockStorageService{},
			repo:        &MockJobRepository{},
			queue:       &MockQueueService{},
			fieldName:   "files",
			filename:    "test.txt",
			contentType: "text/plain",
			content:     []byte("hello"),
			expectCode:  http.StatusBadRequest,
		},
		{
			name: "storage upload error",
			storage: &MockStorageService{
				UploadFileFunc: func(
					ctx context.Context,
					file multipart.File,
					s3Key string,
				) error {
					return fmt.Errorf("S3 error")
				},
			},
			repo:        &MockJobRepository{},
			queue:       &MockQueueService{},
			fieldName:   "files",
			filename:    "test.pdf",
			contentType: "application/pdf",
			content:     []byte("%PDF-1.4"),
			expectCode:  http.StatusInternalServerError,
		},
		{
			name:        "invalid multipart form",
			storage:     &MockStorageService{},
			repo:        &MockJobRepository{},
			queue:       &MockQueueService{},
			fieldName:   "",
			filename:    "",
			contentType: "multipart/form-data; boundary=invalid",
			content:     []byte("bad-data"),
			expectCode:  http.StatusBadRequest,
		},
		{
			name:    "create job db error",
			storage: &MockStorageService{},
			repo: &MockJobRepository{
				CreateJobFunc: func(
					ctx context.Context,
					arg db.CreateJobParams,
				) error {
					return fmt.Errorf("db error")
				},
			},
			queue:       &MockQueueService{},
			fieldName:   "files",
			filename:    "test.pdf",
			contentType: "application/pdf",
			content:     []byte("%PDF-1.4"),
			expectCode:  http.StatusInternalServerError,
		},
		{
			name:    "publish task error",
			storage: &MockStorageService{},
			repo:    &MockJobRepository{},
			queue: &MockQueueService{
				PublishTaskFunc: func(
					ctx context.Context,
					jobID,
					filePath string,
				) error {
					return fmt.Errorf("redis error")
				},
			},
			fieldName:   "files",
			filename:    "test.pdf",
			contentType: "application/pdf",
			content:     []byte("%PDF-1.4"),
			expectCode:  http.StatusInternalServerError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			corsGuard.Lock()
			corsGuard.allowedOrigins["http://localhost:8081"] = true
			corsGuard.Unlock()

			srv := &Server{
				ctx:     context.Background(),
				storage: tc.storage,
				repo:    tc.repo,
				queue:   tc.queue,
			}

			var body bytes.Buffer
			writer := multipart.NewWriter(&body)

			if tc.name == "invalid multipart form" {
				body.Write(tc.content)
			} else {
				h := make(textproto.MIMEHeader)
				h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, tc.fieldName, tc.filename))
				h.Set("Content-Type", tc.contentType)

				part, err := writer.CreatePart(h)
				if err != nil {
					t.Fatal(err)
				}

				_, _ = part.Write(tc.content)
				_ = writer.Close()
			}

			req := httptest.NewRequest("POST", "/upload", &body)
			req.Header.Set("Origin", "http://localhost:8081")

			if tc.name == "invalid multipart form" {
				req.Header.Set("Content-Type", tc.contentType)
			} else {
				req.Header.Set("Content-Type", writer.FormDataContentType())
			}

			rr := httptest.NewRecorder()
			srv.handleUpload(rr, req)

			if rr.Code != tc.expectCode {
				t.Fatalf("expected %d got %d body=%s", tc.expectCode, rr.Code, rr.Body.String())
			}

			if tc.expectBodyHas != "" && !strings.Contains(rr.Body.String(), tc.expectBodyHas) {
				t.Errorf("expected body contains %q got %s", tc.expectBodyHas, rr.Body.String())
			}
		})
	}
}

func TestHandleStatus_Branches(t *testing.T) {
	srv := &Server{ctx: context.Background()}

	srv.queue = &MockQueueService{
		FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
			return []byte(`{"status":"success"}`), nil
		},
	}
	req := httptest.NewRequest("GET", "/status?job_id=job-123", nil)
	rr := httptest.NewRecorder()
	srv.handleStatus(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected status %d on cache hit, got %d", http.StatusOK, rr.Code)
	}

	srv.queue = &MockQueueService{
		FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
			return nil, redis.Nil
		},
	}
	srv.repo = &MockJobRepository{
		GetJobStatusFunc: func(ctx context.Context, jobID string) (string, error) {
			return "processing", nil
		},
	}
	rr2 := httptest.NewRecorder()
	srv.handleStatus(rr2, req)
	if rr2.Code != http.StatusOK {
		t.Errorf("expected status %d when job exists in DB, got %d", http.StatusOK, rr2.Code)
	}

	srv.repo = &MockJobRepository{
		GetJobStatusFunc: func(ctx context.Context, jobID string) (string, error) {
			return "", sql.ErrNoRows
		},
	}
	rr3 := httptest.NewRecorder()
	srv.handleStatus(rr3, req)
	if rr3.Code != http.StatusNotFound {
		t.Errorf("expected status %d for non-existent job, got %d", http.StatusNotFound, rr3.Code)
	}

	srv.repo = &MockJobRepository{
		GetJobStatusFunc: func(ctx context.Context, jobID string) (string, error) {
			return "", fmt.Errorf("some db error")
		},
	}
	rr4 := httptest.NewRecorder()
	srv.handleStatus(rr4, req)
	if rr4.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d on internal DB error, got %d", http.StatusInternalServerError, rr4.Code)
	}
}

func TestHandleDownload_Branches(t *testing.T) {
	srv := &Server{
		ctx:     context.Background(),
		storage: &MockStorageService{},
	}

	req := httptest.NewRequest("GET", "/download", nil)
	rr := httptest.NewRecorder()
	srv.handleDownload(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status %d on missing job_id, got %d", http.StatusBadRequest, rr.Code)
	}

	srv.queue = &MockQueueService{
		FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
			return nil, redis.Nil
		},
	}
	req = httptest.NewRequest("GET", "/download?job_id=job-123", nil)
	rr = httptest.NewRecorder()
	srv.handleDownload(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected status %d on cache miss, got %d", http.StatusNotFound, rr.Code)
	}

	goodPayload := ResultPayload{Status: "success", PdfPath: "outputs/done.pdf"}
	goodBytes, _ := json.Marshal(goodPayload)
	srv.queue = &MockQueueService{
		FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
			return goodBytes, nil
		},
	}
	rr = httptest.NewRecorder()
	srv.handleDownload(rr, req)
	if rr.Code != http.StatusFound {
		t.Errorf("expected status %d (Redirect) on successful download request, got %d", http.StatusFound, rr.Code)
	}

	badPayload := ResultPayload{Status: "failed", Error: "S3 Failure"}
	badBytes, _ := json.Marshal(badPayload)
	srv.queue = &MockQueueService{
		FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
			return badBytes, nil
		},
	}
	rr = httptest.NewRecorder()
	srv.handleDownload(rr, req)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected status %d when backend job failed, got %d", http.StatusUnprocessableEntity, rr.Code)
	}

	srv.queue = &MockQueueService{
		FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
			return []byte("invalid-json-{[-}"), nil
		},
	}
	rr = httptest.NewRecorder()
	srv.handleDownload(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d on corrupted cache JSON, got %d", http.StatusInternalServerError, rr.Code)
	}
}

func TestHandleWebSocket_Flow(t *testing.T) {
	hub := NewHub()
	srv := &Server{
		ctx: context.Background(),
		hub: hub,
		queue: &MockQueueService{
			FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
				return nil, redis.Nil
			},
		},
	}

	s := httptest.NewServer(http.HandlerFunc(srv.handleWebSocket))
	defer s.Close()

	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws?job_id=job-test"
	dialer := websocket.Dialer{}
	header := http.Header{}
	header.Set("Origin", "http://localhost:8081")
	corsGuard.Lock()
	corsGuard.allowedOrigins["http://localhost:8081"] = true
	corsGuard.Unlock()

	ws, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("failed to establish WebSocket connection: %v", err)
	}

	defer ws.Close()
	time.Sleep(15 * time.Millisecond)

	srv.hub.mux.RLock()
	_, exist := srv.hub.connections["job-test"]
	srv.hub.mux.RUnlock()
	if !exist {
		t.Error("expected connection to be registered in Hub under job_id, but it was not found")
	}

	badWsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws"
	_, resp, _ := dialer.Dial(badWsURL, header)
	if resp != nil && resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d on missing job_id parameter, got %d", http.StatusBadRequest, resp.StatusCode)
	}
}

func TestHandleWebSocket_RejectedOrigin(t *testing.T) {
	hub := NewHub()
	srv := &Server{ctx: context.Background(), hub: hub}
	s := httptest.NewServer(http.HandlerFunc(srv.handleWebSocket))
	defer s.Close()

	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws?job_id=job-reject"
	dialer := websocket.Dialer{}
	header := http.Header{}
	header.Set("Origin", "http://untrusted-hacker.com")

	_, resp, err := dialer.Dial(wsURL, header)
	if err == nil {
		t.Error("expected connection to be rejected due to invalid Origin, but it succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		gotStatus := 0
		if resp != nil {
			gotStatus = resp.StatusCode
		}
		t.Errorf("expected HTTP status 403 Forbidden on rejected origin, got: %d", gotStatus)
	}
}

func TestHandleUpload_PublishTaskError(t *testing.T) {
	srv := &Server{
		ctx:     context.Background(),
		storage: &MockStorageService{},
		repo:    &MockJobRepository{},
		queue: &MockQueueService{
			PublishTaskFunc: func(ctx context.Context, jobID, filePath string) error {
				return fmt.Errorf("Redis publish error")
			},
		},
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="files"; filename="test.pdf"`)
	h.Set("Content-Type", "application/pdf")
	part, _ := writer.CreatePart(h)
	_, _ = part.Write([]byte("%PDF-1.4"))
	_ = writer.Close()

	req := httptest.NewRequest("POST", "/upload", body)
	req.Header.Set("Origin", "http://localhost:8081")
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rr := httptest.NewRecorder()

	srv.handleUpload(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d when queue publishing fails, got %d", http.StatusInternalServerError, rr.Code)
	}
}

func TestHandleUpload_CreateJobDBError(t *testing.T) {
	srv := &Server{
		ctx:     context.Background(),
		storage: &MockStorageService{},
		queue:   &MockQueueService{},
		repo: &MockJobRepository{
			CreateJobFunc: func(ctx context.Context, arg db.CreateJobParams) error {
				return fmt.Errorf("DB insert error")
			},
		},
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="files"; filename="test.pdf"`)
	h.Set("Content-Type", "application/pdf")
	part, _ := writer.CreatePart(h)
	_, _ = part.Write([]byte("%PDF-1.4"))
	_ = writer.Close()

	req := httptest.NewRequest("POST", "/upload", body)
	req.Header.Set("Origin", "http://localhost:8081")
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rr := httptest.NewRecorder()

	srv.handleUpload(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d when database insertion fails, got %d", http.StatusInternalServerError, rr.Code)
	}
}

func TestHandleDownload_PresignURLError(t *testing.T) {
	srv := &Server{
		ctx: context.Background(),
		storage: &MockStorageService{
			GeneratePresignedURLFunc: func(ctx context.Context, s3Key string, expires time.Duration) (string, error) {
				return "", fmt.Errorf("S3 presign error")
			},
		},
	}

	goodPayload := ResultPayload{Status: "success", PdfPath: "outputs/done.pdf"}
	goodBytes, _ := json.Marshal(goodPayload)
	srv.queue = &MockQueueService{
		FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
			return goodBytes, nil
		},
	}

	req := httptest.NewRequest("GET", "/download?job_id=job-123", nil)
	rr := httptest.NewRecorder()
	srv.handleDownload(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d when S3 URL presigning fails, got %d", http.StatusInternalServerError, rr.Code)
	}
}

func TestRedisStreamConsumerReceiveJob(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &Server{
		id:   "test-consumer",
		ctx:  ctx,
		rdb:  rdb,
		hub:  NewHub(),
		repo: &MockJobRepository{},
		queue: &MockQueueService{
			FetchResultCacheFunc: func(ctx context.Context, jobID string) ([]byte, error) {
				return nil, fmt.Errorf("cache miss")
			},
		},
	}

	s := httptest.NewServer(http.HandlerFunc(srv.handleWebSocket))
	defer s.Close()
	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws?job_id=job123"
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	srv.startRedisSub()
	time.Sleep(10 * time.Millisecond)

	err = rdb.XAdd(
		ctx,
		&redis.XAddArgs{
			Stream: StreamKey,
			ID:     "*",
			Values: map[string]interface{}{
				"job_id":      "job123",
				"status":      "success",
				"output_path": "test.pdf",
			},
		},
	).Err()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	srv.hub.mux.RLock()
	_, exist := srv.hub.connections["job123"]
	srv.hub.mux.RUnlock()

	if exist {
		t.Error("expected connection to be closed and removed from Hub after job completion notification, but it still exists")
	}
}
