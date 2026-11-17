import http from 'k6/http';
import { sleep, check } from 'k6';

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

// The document under test. Pick another size with -e PDF_FILE=./pdfs/doc_m.pdf (see make_test_pdfs.py).
const pdfFile = __ENV.PDF_FILE || './test.pdf';
const testPDF = open(pdfFile, 'b');

export function uploadFunc() {
  const host = __ENV.TARGET_URL || 'http://localhost:8080';
  const data = {files: http.file(testPDF, 'k6_test_file.pdf', 'application/pdf'),};
  const res = http.post(`${host}/upload`, data);
  check(res, {
    'upload succeeded': (r) => r.status == 200,
  });
  sleep(0.5);
}