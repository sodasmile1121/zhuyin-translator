import http from 'k6/http';
import { sleep, check } from 'k6';

export const options = {
  scenarios: {
    full_chain_breakpoint: {
      executor: 'ramping-vus',
      exec: 'lifeCycle',
      startVUs: 0,
      stages: [
        { duration: '20s', target: 100 },
        { duration: '20s', target: 200 },
        { duration: '20s', target: 400 },
        { duration: '20s', target: 600 },
        { duration: '20s', target: 800 },
        { duration: '30s', target: 1000 },
      ],
      gracefulRampDown: '30s',
    },
  },
};

// The document under test. Pick another size with -e PDF_FILE=./pdfs/doc_m.pdf (see make_test_pdfs.py).
const pdfFile = __ENV.PDF_FILE || './test.pdf';
const testPDF = open(pdfFile, 'b');

export function lifeCycle() {
  const host = __ENV.TARGET_URL || 'http://localhost:8080';

  // 1. Upload API
  const uploadData = { files: http.file(testPDF, 'k6_file.pdf', 'application/pdf') };
  const uploadRes = http.post(`${host}/upload`, uploadData);

  const isUploadOk = check(uploadRes, { 
    'upload success (200)': (r) => r.status === 200 
  });

  if (!isUploadOk) {
    sleep(1);
    return;
  }

  // 安全解析 JSON，避免 Gateway 爆掉回應非 JSON 時導致腳本直接崩潰
  let jobs;
  try {
    jobs = JSON.parse(uploadRes.body);
  } catch (err) {
    return;
  }

  if (!jobs || jobs.length === 0) return;
  const jobId = jobs[0].job_id;

  sleep(0.5);

  // 2. Poll Status (拉長 retries，避免佇列排隊時誤判為失敗)
  let isFinished = false;
  let isSuccess = false;
  const maxRetries = 90; // 給予足夠的 Buffer 觀察佇列消化情況
  let retries = 0;

  while (!isFinished && retries < maxRetries) {
    const statusRes = http.get(`${host}/status?job_id=${jobId}`);
    check(statusRes, { 'status check success (200)': (r) => r.status === 200 });

    try {
      const statusData = JSON.parse(statusRes.body);
      if (statusData.status === 'success' || statusData.char_zy) {
        isFinished = true;
        isSuccess = true;
      } else if (statusData.status === 'failed') {
        isFinished = true;
        isSuccess = false;
      } else {
        retries++;
        sleep(1);
      }
    } catch (err) {
      retries++;
      sleep(1);
    }
  }

  // 檢查 Worker 是否真正完成任務
  check(isSuccess, {
    'job processing succeeded in worker': (s) => s === true,
  });

  // 3. Download
  if (isSuccess) {
    const downloadRes = http.get(`${host}/download?job_id=${jobId}`, { responseType: 'binary' });
    check(downloadRes, { 'download success (200)': (r) => r.status === 200 });
  }

  sleep(1);
}