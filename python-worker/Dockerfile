FROM python:3.10-slim
WORKDIR /app
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc_dir
RUN mkdir -p /tmp/prometheus_multiproc_dir
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p outputs
CMD ["python", "worker.py"]