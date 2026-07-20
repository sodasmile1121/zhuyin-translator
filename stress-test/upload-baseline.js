import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  discardResponseBodies: true, 
  scenarios: {
    upload: {
      executor: 'ramping-vus',
      exec: 'uploadFunc',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 100 },
        { duration: '20s', target: 1000 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '2s',
    },
  },
};

const testPDF = open('./test.pdf', 'b')

export function uploadFunc() {
  const data = {
    files: http.file(testPDF, 'k6_test_file.pdf', 'application/pdf'),
  };
  const host = __ENV.TARGET_URL || 'http://localhost:8080';
  const res = http.post(`${host}/upload`, data);
  console.log(`Response status: ${res.status}`);
  sleep(0.5);
}