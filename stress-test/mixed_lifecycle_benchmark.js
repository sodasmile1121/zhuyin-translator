import http from 'k6/http';
import { sleep, check } from 'k6';

export const options = {
  vus: Number(__ENV.VUS) || 20,
  duration: __ENV.DURATION || '2m',
};

const testPDF = open('./test.pdf', 'b');

export default function () {
  const host = __ENV.TARGET_URL || 'http://localhost:8080';

  const uploadData = {
    files: http.file(testPDF, 'k6_file.pdf', 'application/pdf'),
  };

  const uploadRes = http.post(`${host}/upload`, uploadData);
  if (!check(uploadRes, { 'upload success (200)': (r) => r.status === 200 })) {
    sleep(1);
    return;
  }

  let jobs;
  try {
    jobs = JSON.parse(uploadRes.body);
  } catch (err) {
    sleep(1);
    return;
  }

  if (!jobs || jobs.length === 0) return;
  const jobId = jobs[0].job_id;

  sleep(0.5);

  let isFinished = false;
  let isSuccess = false;
  const maxRetries = 60;
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

  check(isSuccess, {
    'job processing succeeded in worker': (s) => s === true,
  });

  if (isSuccess) {
    const downloadRes = http.get(`${host}/download?job_id=${jobId}`, {
      responseType: 'binary',
    });
    check(downloadRes, { 'download success (200)': (r) => r.status === 200 });
  }

  sleep(1);
}