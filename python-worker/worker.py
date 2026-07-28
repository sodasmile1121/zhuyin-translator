import redis
import json
import os
import traceback
import gzip
import boto3
import requests
import time
import glob
from models.job import PDFJob
from multiprocessing import Process
from processors.pipeline import process_file_task
from services.pdf_generator import generate_zhuyin_pdf
from prometheus_client import start_http_server, Counter, Histogram, CollectorRegistry
from prometheus_client import multiprocess, Gauge

registry = CollectorRegistry()
multiprocess.MultiProcessCollector(registry)

JOB_PROCESSED_TOTAL = Counter('python_worker_jobs_total', 'Total jobs processed', ['status'])
JOB_PROCESS_TIME = Histogram('python_worker_job_duration_seconds', 'Time spent processing job')
ACTIVE_JOBS = Gauge('python_worker_active_jobs', 'Number of currently processing jobs')

redis_addr = os.getenv("REDIS_ADDR", "localhost:6379")
redis_host, redis_port = redis_addr.split(":")
redis_port = int(redis_port)

r_text = redis.Redis(
    host=redis_host, 
    port=redis_port, 
    db=0,
    decode_responses=True,
    health_check_interval=30
)

r_raw = redis.Redis(
    host=redis_host, 
    port=redis_port, 
    db=0,
    decode_responses=False,
    health_check_interval=30
)

s3_client = boto3.client(
    's3',
    region_name=os.getenv('AWS_REGION')
)
BUCKET_NAME = os.getenv('AWS_S3_BUCKET')
TASK_STREAM_KEY = "stream:task"
TASK_GROUP_NAME = "python-worker-group"
DLQ_STREAM_KEY = "stream:task:dlq"
STREAM_KEY = "stream:job"


def download_pdf_file(url: str, local_path: str):
    print(f"[I/O Download] Downloading input file from S3: {url}...")
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise Exception(f"Download failed with status: {response.status_code}")
    with open(local_path, "wb") as f:
        f.write(response.content)
    print(f"[I/O Download] Download completed: {local_path}")

def upload_pdf_to_s3(local_path: str, s3_path: str):
    print(f"[I/O Upload] Uploading output {local_path} to S3 bucket {BUCKET_NAME} path {s3_path}...")
    s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
    print(f"[I/O Upload] S3 Upload completed.")

def save_result_cache(job_id: str, data: dict):
    RESULT_KEY = f"result:{job_id}"
    print(f"[I/O Redis] Saving compressed result cache to {RESULT_KEY}...")
    
    json_bytes = json.dumps(data).encode('utf-8')
    compressed_data = gzip.compress(json_bytes)
    
    pipe = r_raw.pipeline(transaction=False)
    pipe.set(RESULT_KEY, compressed_data, ex=3600)
    pipe.execute()
    print(f"[I/O Redis] Result cache written successfully.")

def clean_temp_files(*paths):
    for path in paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
                print(f"[Cleanup] Safely removed temp file: {path}")
            except Exception as e:
                print(f"[Cleanup Warning] Failed to remove {path}: {e}")

def run_translation_pipeline(input_path: str, output_path: str) -> dict:
    print(f"[Engine] Core translation engine starting...")
    result_data = process_file_task(input_path) 
    print(f"[Engine] Core PDF rendering starting...")
    generate_zhuyin_pdf(output_path, result_data['char_zy'])
    return result_data

def handle_single_job_logic(job_id: str, file_path: str) -> dict:
    local_input = f"/tmp/{job_id}_input.pdf"
    local_output = f"/tmp/{job_id}_output.pdf"
    s3_output_path = f"outputs/{job_id}_translated.pdf"
    
    try:
        download_pdf_file(file_path, local_input)
        result_data = run_translation_pipeline(local_input, local_output)
        upload_pdf_to_s3(local_output, s3_output_path)
        result_data["pdf_path"] = s3_output_path
        result_data["error"] = ""
        save_result_cache(job_id, result_data)
        return {
            "status": "success", 
            "s3_output_path": s3_output_path
        }
    finally:
        clean_temp_files(local_input, local_output)


def worker_task(worker_id):
    consumer_name = f"worker-{worker_id}"
    print(f"Worker {worker_id} (PID: {os.getpid()}) starts as {consumer_name}")
    start_id = "0-0"
    while True:
        try:
            next_start_id, message, _ = r_text.xautoclaim(
                TASK_STREAM_KEY, TASK_GROUP_NAME, consumer_name, 
                min_idle_time=60000, start_id=start_id, count=1
            )
            start_id = next_start_id
            if message:
                message_id, message_values = message[0]
                if not message_values:
                    print(f"[{consumer_name}] Found ghost pending message {message_id}, acknowledging to purge.")
                    r_text.xack(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id)
                    continue
                job_id = message_values.get("job_id")
                file_path = message_values.get("file_path")
                print(f"Retrieve task from pending task: {message_id}, Job: {job_id}")
                if not job_id or not file_path:
                    print(f"[{consumer_name}] Invalid pending payload, acking to clear. ID: {message_id}")
                    r_text.xack(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id)
                    continue
            else:
                response = r_text.xreadgroup(
                    groupname=TASK_GROUP_NAME,
                    consumername=consumer_name,
                    streams={TASK_STREAM_KEY: ">"},
                    count=1,
                    block=10000
                )
                if not response:
                    continue
                _, messages = response[0]
                message_id, message_values = messages[0]
                job_id = message_values.get("job_id")
                file_path = message_values.get("file_path")
                if not job_id or not file_path:
                    print(f"[{consumer_name}] Invalid message payload, acking to clear. ID: {message_id}")
                    r_text.xack(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id)
                    continue

            try:
                ACTIVE_JOBS.inc()
                with JOB_PROCESS_TIME.time():
                    job_res = handle_single_job_logic(job_id, file_path)
                JOB_PROCESSED_TOTAL.labels(status='success').inc()
                r_text.xadd(STREAM_KEY, {'job_id': job_id, 'status': 'success', 'output_path': job_res['s3_output_path']})
                r_text.xack(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id)
                print(f"{job_id} completed and ACKed.")

            except Exception as exc:
                JOB_PROCESSED_TOTAL.labels(status='failed').inc()
                print(f"Job {job_id} encountered an error: {exc}")
                traceback.print_exc()
                
                pending_info = r_text.xpending_range(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id, message_id, 1)
                
                delivery_count = 1
                if pending_info:
                    delivery_count = pending_info[0].get('times_delivered', 1)
                if delivery_count < 3:
                    print(f"Retry job {job_id} later. Delivery count: {delivery_count}")
                    time.sleep(2) 
                else:
                    print(f"Job {job_id} failed 3 times. Evicting to DLQ...")
                    r_text.xadd(DLQ_STREAM_KEY, {
                        "job_id": job_id, 
                        "file_path": file_path, 
                        "error": str(exc),
                        "failed_consumer": consumer_name
                    })
                    error_payload = {"status": "failed", "pdf_path": "", "error": str(exc), "preview": "", "char_zy": []}
                    err_bytes = json.dumps(error_payload).encode('utf-8')
                    r_raw.set(f"result:{job_id}", gzip.compress(err_bytes), ex=3600)
                    r_text.xadd(STREAM_KEY, {'job_id': job_id, 'status': 'failed', 'output_path': ''})
                    r_text.xack(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id)
                    print(f"{job_id} moved to DLQ and ACKed.")
            finally:
                ACTIVE_JOBS.dec()

        except Exception as queue_err:
            print(f"Worker-{worker_id} abnormal queue connection : {queue_err}")
            time.sleep(2)


if __name__ == '__main__':
    prometheus_dir = os.environ.get('PROMETHEUS_MULTIPROC_DIR')
    if prometheus_dir and os.path.exists(prometheus_dir):
        for f in glob.glob(os.path.join(prometheus_dir, '*.db')):
            try:
                os.remove(f)
            except Exception as e:
                print(f"[Prometheus Cleanup Warning] {e}")

    try:
        start_http_server(8000, registry=registry)
        print("[Prometheus] Main process started metrics server on port 8000 (Multi-process mode)")
    except Exception as e:
        print(f"[Prometheus Error] Failed to start metrics server: {e}")

    try:
        r_text.xgroup_create(TASK_STREAM_KEY, TASK_GROUP_NAME, id="$", mkstream=True)
        print(f"Consumer Group '{TASK_GROUP_NAME}' created successfully.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Consumer Group '{TASK_GROUP_NAME}' already exists. Skipping creation.")
        else:
            raise e

    num_workers = 3
    processes = []

    for i in range(num_workers):
        p = Process(target=worker_task, args=(i+1,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()