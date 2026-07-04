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
        { duration: '10s', target: 200 },
        { duration: '10s', target: 400 },
        { duration: '10s', target: 600 },
        { duration: '10s', target: 800 },
        { duration: '20s', target: 1000 }
      ],
      gracefulRampDown: '2s',
    },
  },
};

const testPDF = open('../test.pdf', 'b')

export function uploadFunc() {
  const data = {
    files: http.file(testPDF, 'k6_test_file.pdf', 'application/pdf'),
  };
  const res = http.post('http://localhost:8080/upload', data);
  sleep(0.5);
}