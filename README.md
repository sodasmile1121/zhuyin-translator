# Distributed Document Processing System

An event-driven distributed document processing system for asynchronous PDF translation, built with **Go**, **Python**, **Redis Streams**, **PostgreSQL**, and **AWS S3**.

The system decouples request handling from document processing using Redis Streams Consumer Groups, enabling scalable background processing, fault tolerance, and real-time job tracking.

---

## Demo

![App Demo](./docs/demo.gif)

---

## Highlights

### Scalable Processing
- Event-driven architecture using Redis Streams Consumer Groups
- Asynchronous document processing with a Go Gateway and Python worker pool

### Fault Tolerance
- Automatic retry mechanism
- Dead Letter Queue (DLQ)
- Pending message recovery using `XAUTOCLAIM`

### Distributed Storage
- AWS S3 for document storage
- PostgreSQL for job metadata
- Redis cache with Gzip-compressed translation results

### DevOps
- Docker Compose deployment
- Automated CI/CD with GitHub Actions

### Observability *(Work in Progress)*
- Prometheus metrics
- Grafana dashboards
- k6 load testing scripts

---

# System Architecture

```mermaid
flowchart TD
    Client["Client / Frontend
    (HTML / JS)"]

    Gateway["Go API Gateway
    (Upload / Status / WS / Download)"]

    subgraph Messaging ["Redis Streams & Cache"]
        TaskStream["stream:task
        (Task Queue)"]
        JobStream["stream:job
        (Result Event Stream)"]
        Cache[("Redis Cache
        result:job_id (Gzip)")]
    end

    subgraph Workers ["Python Worker Pool"]
        Worker["Python Workers
        (PyMuPDF & AI Processing)"]
    end

    DLQ["stream:task:dlq
    (Dead Letter Queue)"]

    PG[("PostgreSQL
    Job Metadata")]

    S3[("AWS S3
    PDF Storage")]

    %% 1. 上傳與任務派發流程
    Client -->|1. Upload PDF| Gateway
    Gateway -->|2. Store Input PDF| S3
    Gateway -->|3. Create Job| PG
    Gateway -->|4. XADD Task| TaskStream

    %% 2. Python Worker 處理流程
    TaskStream -->|5. Fetch Task| Worker
    Worker -->|6. Download Input & Upload Output| S3
    Worker -->|7. Cache Gzip Result| Cache
    Worker -->|8. Publish Completion| JobStream

    Worker -. Retry Mechanism .-> TaskStream
    Worker -. Failed 3x .-> DLQ

    %% 3. 即時推播通知流程 (把 10 的箭頭修正為 Gateway -> Client)
    JobStream -->|9. Consumer Group Read| Gateway
    Gateway -->|10. Push Completion Event| Client
    Client <-->|WebSocket Conn /ws| Gateway

    %% 4. Status 查詢與 Download 流程 (把 Redirect 修正為 Gateway -> Client)
    Gateway -->|Read Result Cache| Cache
    Gateway -->|Fallback Status| PG
    Gateway -->|Generate Download Link| S3

    Client -. Rest Fetch Status /status .-> Gateway
    Gateway -. Redirect Download /download .-> Client
    Client -.->|Direct Download via Presigned URL| S3
```

---

# Tech Stack

| Layer | Technology |
|--------|------------|
| API Gateway | Go |
| Background Worker | Python |
| Message Queue | Redis Streams |
| Database | PostgreSQL |
| Object Storage | AWS S3 |
| Cache | Redis |
| Monitoring | Prometheus |
| Dashboard | Grafana |
| Load Testing | k6 |
| Containerization | Docker |
| CI/CD | GitHub Actions |

---

# Processing Flow

### 1. Upload

The client uploads a PDF through the Go API Gateway.

- PDF is uploaded to AWS S3
- Job metadata is stored in PostgreSQL
- A processing task is appended to Redis Streams

---

### 2. Background Processing

Python workers consume tasks through Redis Streams Consumer Groups.

Each worker:

- downloads the PDF from S3
- extracts document contents
- generates Zhuyin annotations
- renders the translated PDF
- uploads the translated document back to S3

---

### 3. Result Publication

After processing,

- translation results are compressed with Gzip
- cached in Redis
- completion events are published back to Redis Streams
- the Gateway updates the job status

Clients can track progress via:
- WebSocket notifications: Instant push upon job completion.
- REST API (`GET /status`): Polls Redis cache with a database fallback for current job status.

---

### 4. File Download

Clients request document download via the Go API Gateway (`GET /download`).

- Gateway verifies job status from Redis Cache.
- Gateway generates a short-lived (10-minute) **AWS S3 Presigned URL**.
- Gateway responds with an HTTP 302 Redirect to direct the client to download the rendered PDF straight from AWS S3, offloading file transfer traffic from the API Gateway.

---

# Features

## Event-driven Architecture

The system separates request handling from document processing by introducing Redis Streams between the API Gateway and worker pool.

This design enables

- asynchronous execution
- worker scalability
- loose coupling between services

---

## Fault Tolerance

Workers implement several reliability mechanisms.

### Consumer Groups

Multiple workers consume tasks concurrently while ensuring each job is processed only once.

### Retry

Failed jobs remain in the Pending Entries List and are retried automatically.

### Pending Recovery

Workers periodically reclaim abandoned tasks using

```
XAUTOCLAIM
```

to recover jobs from crashed workers.

### Dead Letter Queue

Jobs that exceed the retry threshold are moved into a Dead Letter Queue for further inspection instead of being discarded.

---

## Distributed Storage

### AWS S3

Stores

- uploaded PDFs
- translated PDFs

### PostgreSQL

Stores

- job status
- S3 object locations
- metadata

### Redis Cache

Stores compressed translation results for fast status retrieval.

---

## Real-time Job Tracking

Clients can monitor translation progress through

- REST polling
- WebSocket notifications

without waiting for synchronous processing.

---

## CI/CD

The project uses GitHub Actions to

- Run Python and Go linters
- Execute Python unit tests (`pytest`) and Go unit tests (`go test`)
- Build Docker images for Gateway and Worker services
- Publish tagged images to GitHub Container Registry (GHCR)
- Automatically deploy updated containers to AWS EC2

---

# Reliability Design

This project implements several production-inspired backend patterns.

| Feature | Implementation |
|----------|----------------|
| Message Queue | Redis Streams |
| Consumer Group | ✓ |
| Retry | ✓ |
| Pending Recovery | XAUTOCLAIM |
| Dead Letter Queue | ✓ |
| Object Storage | AWS S3 |
| Metadata Database | PostgreSQL |
| Result Cache | Redis |
| Gzip Compression | ✓ |
| Multi-process Workers | ✓ |

---

# Testing

Current testing includes

- Python unit tests
- Go unit tests
- End-to-end API validation
- GitHub Actions automated CI

The repository also contains k6 load testing scripts together with Prometheus and Grafana configuration for system observability.

Load testing scenarios and monitoring dashboards are currently under continuous refinement.

---

# Project Structure

```
.
├── go-gateway/                 # Go API Gateway (HTTP & WebSocket Server)
│   ├── db/                     # sqlc generated database code
│   ├── sql/                    # SQL queries and schema
│   ├── index.html              # Static frontend page
│   ├── main.go                 # Gateway entry point & handlers
│   ├── main_test.go            # Go unit tests
│   ├── sqlc.yaml               # sqlc configuration
│   └── Dockerfile
│
├── python-worker/              # Python Background Worker (PDF Processing)
│   ├── models/                 # Data models (Job schema)
│   ├── processors/             # Core processing pipeline & AI engines
│   ├── services/               # PDF rendering & helper services
│   ├── fonts/                  # Custom fonts for rendering
│   ├── worker.py               # Worker entry point (Consumer Group logic)
│   ├── test_worker.py          # Python unit tests
│   ├── requirements.txt
│   └── Dockerfile
│
├── stress-test/                # k6 Performance & Load Testing
│   ├── breakpoint_test.js
│   ├── mixed_lifecycle_test.js
│   └── upload-baseline.js
│
├── prometheus/                 # Prometheus metrics configuration
├── grafana/                    # Grafana dashboard definitions
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI/CD Pipeline
│
├── init.sql                    # PostgreSQL database initialization script
└── docker-compose.yml          # Multi-container local deployment

```
---
# Quick Start

### Prerequisites
- [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- AWS Account with an active S3 Bucket and API credentials

---

### Local Setup & Execution

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/sodasmile1121/zhuyin-translator.git](https://github.com/sodasmile1121/zhuyin-translator.git)
   cd zhuyin-translator/
   ```

2. **Configure Environment Variables:**
    ```bash
    # AWS Settings
    AWS_REGION=us-east-1
    AWS_S3_BUCKET=your-pdf-bucket-name

    # Optional: Only needed for local testing without IAM Role
    # On AWS EC2 with IAM Role attached, leave these commented out.
    # AWS_ACCESS_KEY_ID=your-aws-access-key
    # AWS_SECRET_ACCESS_KEY=your-aws-secret-key

    # Database & Redis Settings
    DATABASE_URL=postgres://postgres:postgres@postgres:5432/pdf_db?sslmode=disable
    REDIS_ADDR=redis:6379

    # CORS Settings
    ALLOWED_ORIGINS=http://localhost:8081
    ```


3. **Start All Services:**
    ```bash
    docker compose up --build -d
    ```


4. **Verify Application Status:**
* Web UI & API Gateway: Open `http://localhost:8081` in your browser to test PDF uploads.
* Prometheus Metrics: `http://localhost:9090`
* Grafana Dashboard: `http://localhost:3000`


5. **Stop All Services:**
    ```bash
    docker compose down
    ```
---

# Future Improvements

- Kubernetes deployment
- Horizontal worker auto scaling
- OpenTelemetry distributed tracing
- Rate limiting
- Performance benchmarking under large-scale concurrent workloads
- Distributed metrics aggregation

---

# License

This project is intended for educational and portfolio purposes.