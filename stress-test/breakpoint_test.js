import http from 'k6/http';
import { sleep, check } from 'k6';

export const options = {
  scenarios: {
    full_chain_breakpoint: {
      executor: 'ramping-vus',
      exec: 'lifeCycle',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 100 },
        { duration: '10s', target: 200 },
        { duration: '10s', target: 400 },
        { duration: '10s', target: 600 },
        { duration: '10s', target: 800 },
        { duration: '20s', target: 1000 },
      ],
      gracefulRampDown: '30s',
    },
  },
};

const testPDF = open('./test.pdf', 'b');

export function lifeCycle () {
  const host = __ENV.TARGET_URL || 'http://localhost:8080';

  const uploadData = { files: http.file(testPDF, 'k6_file.pdf', 'application/pdf') };
  const uploadRes = http.post(`${host}/upload`, uploadData);
  
  if (!check(uploadRes, { 'upload success (200)': (r) => r.status === 200 })) {
    sleep(1);
    return;
  }

  const jobs = JSON.parse(uploadRes.body);
  if (!jobs || jobs.length === 0) return;
  const jobId = jobs[0].job_id;

  sleep(0.5);

  let isFinished = false;
  let retries = 10;

  while (!isFinished && retries > 0) {
    const statusRes = http.get(`${host}/status?job_id=${jobId}`);  
    check(statusRes, { 'status check success (200)': (r) => r.status === 200 });

    try {
      const statusData = JSON.parse(statusRes.body);
      if (statusData.status === 'success' || statusData.status === 'failed' || statusData.char_zy) {
        isFinished = true;
      } else {
        retries--;
        sleep(1);
      }
    } catch (err) {
      retries--;
      sleep(1);
    }
  }


  if (isFinished) {
    const downloadRes = http.get(`${host}/download?job_id=${jobId}`, { responseType: 'binary' });
    check(downloadRes, { 'download success (200)': (r) => r.status === 200 });
  }

  sleep(1);
}