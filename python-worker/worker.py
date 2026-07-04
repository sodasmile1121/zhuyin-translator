import redis
import json
import os
import traceback
import gzip
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

QUEUE_KEY = "queue:job"
STREAM_KEY = "stream:job"

def worker_task(worker_id):
    print(f"Worker {worker_id} (PID: {os.getpid()}) starts")
    while True:
        try:
            item = r_text.brpop([QUEUE_KEY], timeout=10)
            if not item:
                continue
            _, raw_job = item
            payload = json.loads(raw_job)
            job = PDFJob(**payload)
            RESULT_KEY = f"result:{job.job_id}"
            print(f"Worker {worker_id} starts job {job.job_id}")
            try:
                result_data = process_file_task(job.file_path) 
                pdf_path = f"outputs/{job.job_id}.pdf"
                generate_zhuyin_pdf(pdf_path, result_data['char_zy'])
                result_data["pdf_path"] = pdf_path
                json_bytes = json.dumps(result_data).encode('utf-8')
                compressed_data = gzip.compress(json_bytes)
                pipe = r_raw.pipeline(transaction=False)
                pipe.set(RESULT_KEY, compressed_data, ex=3600)
                pipe.xadd(STREAM_KEY, {'job_id': job.job_id, 'status': 'success'})
                pipe.execute()
                print(f"{job.job_id} completed. The result is written back to Redis.")
            except Exception as exc:
                error_payload = {"status": "failed", "error": str(exc)}
                err_bytes = json.dumps(error_payload).encode('utf-8')
                pipe = r_raw.pipeline(transaction=False)
                pipe.set(RESULT_KEY, gzip.compress(err_bytes), ex=3600)
                pipe.xadd(STREAM_KEY, {'job_id': job.job_id, 'status': 'fail'})
                pipe.execute()
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