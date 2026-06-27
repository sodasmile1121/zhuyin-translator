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

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	rdb *redis.Client
	ctx context.Context
}

type JobInfo struct {
	JobID    string `json:"job_id"`
	FilePath string `json:"file_path"`
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
	srv := &Server{rdb: rdb, ctx: ctx}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./index.html")
	})
	http.HandleFunc("/upload", srv.handleUpload)
	http.HandleFunc("/status", srv.handleStatus)
	fmt.Println("Server is running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
