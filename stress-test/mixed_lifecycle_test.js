import http from 'k6/http';
import { sleep, check } from 'k6';

const profiles = {
  mixed: [
    { duration: '10s', target: 60 },
    { duration: '20s', target: 600 },
    { duration: '10s', target: 0 },
  ],
  soak: [
    { duration: '20s', target: 30 },
    { duration: '5m', target: 30 },
    { duration: '20s', target: 0 },
  ]
};

const testType = __ENV.TEST_TYPE || 'mixed';

export const options = {
  scenarios: {
    user_lifecycle: {
      executor: 'ramping-vus',
      exec: 'lifeCycle',
      stages: profiles[testType], 
      gracefulRampDown: '6s',
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