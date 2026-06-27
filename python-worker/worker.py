import redis
import json
import os
import asyncio
import traceback
from models.job import PDFJob
from multiprocessing import Process
from processors.pipeline import process_file_task


r = redis.Redis(
    host='localhost', 
    port=6379, 
    db=0,
    decode_responses=True,
    health_check_interval=30
)

JOB_QUEUE = "queue:job"

def worker_task(worker_id):
    print(f"Worker {worker_id} (PID: {os.getpid()}) starts")
    while True:
        try:
            item = r.brpop([JOB_QUEUE], timeout=10)
            if not item:
                continue
            _, raw_job = item
            payload = json.loads(raw_job)
            job = PDFJob(**payload)
            RESULT_KEY = f"result:{job.job_id}"
            print(f"Worker {worker_id} starts job {job.job_id}")
            try:
                result_data = process_file_task(job.file_path) 
                r.set(RESULT_KEY, json.dumps(result_data), ex=3600)
                r.publish('job_status_channel', json.dumps({'job_id': job.job_id, 'status': 'success'}))
                print(f"{job.job_id} completed. The result is written back to Redis.")
            except Exception as exc:
                error_payload = {"status": "failed", "error": str(exc)}
                r.set(RESULT_KEY, json.dumps(error_payload), ex=3600)
                r.publish('job_status_channel', json.dumps({'job_id': job.job_id, 'status': 'fail'}))
                print(f"{job.job_id} failed.")
                traceback.print_exc()
        except Exception as queue_err:
            print(f"Worker-{worker_id} abnormal queue connection : {queue_err}")


if __name__ == '__main__':
    num_workers = 3
    processes = []

    for i in range(num_workers):
        p = Process(target=worker_task, args=(i+1,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()