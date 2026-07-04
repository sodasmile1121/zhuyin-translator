import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  discardResponseBodies: true, 
  scenarios: {
    upload: {
      executor: 'ramping-vus',
      exec: 'uploadFunc',
      stages: [
        { duration: '10s', target: 60 },
        { duration: '20s', target: 600 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '6s',
    },
    poll: {
      executor: 'ramping-vus',
      exec: 'pollFunc',
      stages: [
        { duration: '10s', target: 30 },
        { duration: '20s', target: 300 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '3s',
    },
    download: {
      executor: 'ramping-vus',
      exec: 'downloadFunc',
      stages: [
        { duration: '10s', target: 10 },
        { duration: '20s', target: 100 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '1s',
    },
  },
};

const testPDF = open('./test.pdf', 'b')

export function uploadFunc() {
  const data = {
    files: http.file(testPDF, 'k6_test_file.pdf', 'application/pdf'),
  };
  const res = http.post('http://localhost:8080/upload', data);
  sleep(0.5);
}

export function pollFunc() {
  const res = http.post('http://localhost:8080/status');
  sleep(0.5);
}

export function downloadFunc() {
  const res = http.post('http://localhost:8080/download');
  sleep(0.5);
}