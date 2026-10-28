import os
import tempfile

# worker.py reads these at import time, so they must be set before any test module imports it.
os.environ.setdefault("REDIS_ADDR", "localhost:6379")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_S3_BUCKET", "test-bucket")
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", tempfile.mkdtemp())
