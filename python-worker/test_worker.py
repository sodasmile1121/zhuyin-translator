import unittest
from unittest.mock import patch, MagicMock
import os

os.environ.setdefault("REDIS_ADDR", "localhost:6379")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_S3_BUCKET", "test-bucket")


import worker


class TestHandleSingleJobLogic(unittest.TestCase):

    @patch("worker.clean_temp_files")
    @patch("worker.save_result_cache")
    @patch("worker.upload_pdf_to_s3")
    @patch("worker.run_translation_pipeline")
    @patch("worker.download_pdf_file")
    def test_success(
        self,
        mock_download,
        mock_pipeline,
        mock_upload,
        mock_cache,
        mock_clean
    ):
        mock_pipeline.return_value = {
            "status": "success",
            "path": "input.pdf",
            "preview": "測試預覽文字",
            "char_zy": ["測試"]
        }
        result = worker.handle_single_job_logic("job-success", "input.pdf")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["s3_output_path"], "outputs/job-success_translated.pdf")
        mock_download.assert_called_once()
        mock_pipeline.assert_called_once()
        mock_upload.assert_called_once()
        mock_cache.assert_called_once()
        mock_clean.assert_called_once()

    @patch("worker.clean_temp_files")
    @patch("worker.run_translation_pipeline")
    @patch("worker.download_pdf_file")
    def test_pipeline_error(
        self,
        mock_download,
        mock_pipeline,
        mock_clean
    ):
        mock_pipeline.side_effect = Exception("pipeline failed")
        with self.assertRaises(Exception):
            worker.handle_single_job_logic("job-pipeline-error", "input.pdf")
        mock_clean.assert_called_once()

    @patch("worker.clean_temp_files")
    @patch("worker.upload_pdf_to_s3")
    @patch("worker.run_translation_pipeline")
    @patch("worker.download_pdf_file")
    def test_upload_error(
        self,
        mock_download,
        mock_pipeline,
        mock_upload,
        mock_clean
    ):
        mock_pipeline.return_value = {
            "status": "success",
            "path": "input.pdf",
            "preview": "測試預覽文字",
            "char_zy": ["測試"]
        }
        mock_upload.side_effect = Exception("upload failed")
        with self.assertRaises(Exception):
            worker.handle_single_job_logic("job-upload-error", "input.pdf")
        mock_clean.assert_called_once()


class TestWorkerStream(unittest.TestCase):
    @patch("worker.r_text")
    def test_ghost_payload_ack(
        self,
        mock_redis
    ):
        mock_redis.xautoclaim.side_effect = [("0-0", [("msg-ghost", {})], None), KeyboardInterrupt()]
        try:
            worker.worker_task(1)
        except KeyboardInterrupt:
            pass
        mock_redis.xack.assert_called_with("stream:task", "python-worker-group", "msg-ghost")

    @patch("worker.r_text")
    def test_invalid_payload_ack(
        self,
        mock_redis
    ):
        payload = {"file_path": "test.pdf"}
        mock_redis.xautoclaim.side_effect = [("0-0", [("msg-invalid", payload)], None), KeyboardInterrupt()]
        try:
            worker.worker_task(1)
        except KeyboardInterrupt:
            pass
        mock_redis.xack.assert_called_with(
            "stream:task",
            "python-worker-group",
            "msg-invalid"
        )

    @patch("worker.time.sleep")
    @patch("worker.handle_single_job_logic")
    @patch("worker.r_text")
    def test_retry_before_dlq(
        self,
        mock_redis,
        mock_logic,
        mock_sleep
    ):
        mock_redis.xautoclaim.side_effect = [
            ("0-0", [("msg-retry", {"job_id":"job1", "file_path":"file.pdf"})], None), 
            KeyboardInterrupt()
        ]
        mock_logic.side_effect = Exception("temporary error")
        mock_redis.xpending_range.return_value = [{"times_delivered": 1}]
        try:
            worker.worker_task(1)
        except KeyboardInterrupt:
            pass
        mock_sleep.assert_called_with(2)
        for args in mock_redis.xadd.call_args_list:
            self.assertNotEqual(args[0][0], "stream:task:dlq")

    @patch("worker.r_text")
    @patch("worker.handle_single_job_logic")
    def test_third_failure_go_dlq(
        self,
        mock_logic,
        mock_redis
    ):
        mock_redis.xautoclaim.side_effect = [
            ("0-0", [("msg-fail", {"job_id":"job-dead", "file_path":"file.pdf"})], None), 
            KeyboardInterrupt()
        ]
        mock_logic.side_effect = Exception("broken")
        mock_redis.xpending_range.return_value = [{"times_delivered": 3}]
        try:
            worker.worker_task(1)
        except KeyboardInterrupt:
            pass

        mock_redis.xadd.assert_any_call(
            "stream:task:dlq",
            {
                "job_id":"job-dead",
                "file_path":"file.pdf",
                "error":"broken",
                "failed_consumer":"worker-1"
            }
        )
        mock_redis.xack.assert_called_with("stream:task", "python-worker-group", "msg-fail")

    @patch("worker.r_text")
    @patch("worker.handle_single_job_logic")
    def test_success_publish_result(
        self,
        mock_logic,
        mock_redis
    ):
        mock_redis.xautoclaim.side_effect = [
            ("0-0", [("msg-success", {"job_id":"job-ok", "file_path":"file.pdf"})], None), 
            KeyboardInterrupt()
        ]
        mock_logic.return_value = {"status": "success", "s3_output_path": "outputs/job-ok.pdf"}

        try:
            worker.worker_task(1)
        except KeyboardInterrupt:
            pass

        mock_redis.xadd.assert_any_call(
            "stream:job",
            {
                "job_id":"job-ok",
                "status":"success",
                "output_path": "outputs/job-ok.pdf"
            }
        )
        mock_redis.xack.assert_called()


if __name__ == "__main__":
    unittest.main()