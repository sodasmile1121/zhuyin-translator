import redis
import json
import os
import traceback
import gzip
import boto3
import requests
import time
from models.job import PDFJob
from multiprocessing import Process
from processors.pipeline import process_file_task
from services.pdf_generator import generate_zhuyin_pdf


redis_addr = os.getenv("REDIS_ADDR")
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
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
BUCKET_NAME = os.getenv('AWS_S3_BUCKET')
TASK_STREAM_KEY = "stream:task"
TASK_GROUP_NAME = "python-worker-group"
DLQ_STREAM_KEY = "stream:task:dlq"
STREAM_KEY = "stream:job"

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

            RESULT_KEY = f"result:{job_id}"
            print(f"Worker {worker_id} starts job {job_id}")
            local_input_path = f"/tmp/{job_id}_input.pdf"
            pdf_path = f"/tmp/{job_id}_output.pdf"
            try:
                print(f"Worker {worker_id} downloading input file via Presigned URL...")
                response = requests.get(file_path, timeout=30)
                if response.status_code != 200:
                    raise Exception(f"Failed to download input from S3, status code: {response.status_code}")
                with open(local_input_path, "wb") as f:
                    f.write(response.content)
                result_data = process_file_task(local_input_path) 
                generate_zhuyin_pdf(pdf_path, result_data['char_zy'])
                s3_output_path = f"outputs/{job_id}_translated.pdf"
                s3_client.upload_file(pdf_path, BUCKET_NAME, s3_output_path)

                result_data["pdf_path"] = s3_output_path
                result_data["error"] = ""
                json_bytes = json.dumps(result_data).encode('utf-8')
                compressed_data = gzip.compress(json_bytes)

                pipe = r_raw.pipeline(transaction=False)
                pipe.set(RESULT_KEY, compressed_data, ex=3600)
                pipe.execute()

                r_text.xadd(STREAM_KEY, {'job_id': job_id, 'status': 'success', 'output_path': s3_output_path})
                r_text.xack(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id)
                print(f"{job_id} completed and ACKed.")

            except Exception as exc:
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
                    pipe = r_raw.pipeline(transaction=False)
                    pipe.set(RESULT_KEY, gzip.compress(err_bytes), ex=3600)
                    pipe.execute()
                    r_text.xadd(STREAM_KEY, {'job_id': job_id, 'status': 'failed', 'output_path': ''})
                    r_text.xack(TASK_STREAM_KEY, TASK_GROUP_NAME, message_id)
                    print(f"{job_id} moved to DLQ and ACKed.")
                
            finally:
                for path in [local_input_path, pdf_path]:
                    if os.path.exists(path):
                        os.remove(path)

        except Exception as queue_err:
            print(f"Worker-{worker_id} abnormal queue connection : {queue_err}")
            time.sleep(2)


if __name__ == '__main__':
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