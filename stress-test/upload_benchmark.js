import http from 'k6/http';
import { sleep, check } from 'k6';

export const options = {
  vus: Number(__ENV.VUS) || 10,
  duration: '2m',
};

// The document under test. Pick another size with -e PDF_FILE=./pdfs/doc_m.pdf (see make_test_pdfs.py).
const pdfFile = __ENV.PDF_FILE || './test.pdf';
const testPDF = open(pdfFile, 'b');

export default function () {
  const host = __ENV.TARGET_URL || 'http://localhost:8080';
  const data = {
    files: http.file(testPDF, 'k6_test_file.pdf', 'application/pdf'),
  };

  const res = http.post(`${host}/upload`, data);
  check(res, {
    'upload succeeded': (r) => r.status == 200,
  });
  sleep(1);
}