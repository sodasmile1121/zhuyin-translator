import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  vus: Number(__ENV.VUS) || 10,
  duration: '2m',
};

const testPDF = open('./test.pdf', 'b');

export default function () {
  const host = __ENV.TARGET_URL || 'http://localhost:8080';
  const data = {
    files: http.file(testPDF, 'k6_test_file.pdf', 'application/pdf'),
  };
  http.post(`${host}/upload`, data);
  sleep(1);
}