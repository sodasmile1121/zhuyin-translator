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

    @patch("worker.r_raw")
    @patch("worker.r_text")
    @patch("worker.handle_single_job_logic")
    def test_third_failure_go_dlq(
        self,
        mock_logic,
        mock_redis,
        mock_raw
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
                "failed_consumer":"worker-1",
                "message_id":"msg-fail"
            }
        )
        mock_redis.xadd.assert_any_call(
            "stream:job",
            {"job_id": "job-dead", "status": "failed", "output_path": ""}
        )
        self.assertEqual(mock_raw.set.call_args[0][0], "result:job-dead")
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

class TestRunOnceReliability(unittest.TestCase):

    @patch("worker.r_raw")
    @patch("worker.r_text")
    def test_malformed_message_goes_to_dlq(self, mock_redis, mock_raw):
        mock_redis.xautoclaim.return_value = ("0-0", [("msg-bad", {"job_id": "job-x"})], None)

        worker.run_once("worker-1", "0-0")

        dlq_call = mock_redis.xadd.call_args_list[0]
        self.assertEqual(dlq_call[0][0], "stream:task:dlq")
        self.assertIn("missing job_id or file_path", dlq_call[0][1]["error"])
        mock_redis.xack.assert_called_with("stream:task", "python-worker-group", "msg-bad")

    @patch("worker.r_raw")
    @patch("worker.r_text")
    @patch("worker.handle_single_job_logic")
    def test_task_delivered_too_often_is_not_processed_again(self, mock_logic, mock_redis, mock_raw):
        mock_redis.xautoclaim.return_value = ("0-0", [("msg-crash", {"job_id": "job-y", "file_path": "f.pdf"})], None)
        mock_redis.xpending_range.return_value = [{"times_delivered": worker.MAX_DELIVERIES + 1}]

        worker.run_once("worker-1", "0-0")

        mock_logic.assert_not_called()
        mock_redis.xadd.assert_any_call(
            "stream:job", {"job_id": "job-y", "status": "failed", "output_path": ""}
        )
        mock_redis.xack.assert_called_with("stream:task", "python-worker-group", "msg-crash")

    @patch("worker.r_text")
    def test_idle_returns_next_cursor(self, mock_redis):
        mock_redis.xautoclaim.return_value = ("5-0", [], None)
        mock_redis.xreadgroup.return_value = []

        self.assertEqual(worker.run_once("worker-1", "0-0"), "5-0")


class TestSupervisor(unittest.TestCase):

    def test_dead_worker_is_replaced_and_alive_one_is_kept(self):
        dead, alive, replacement = MagicMock(), MagicMock(), MagicMock()
        dead.is_alive.return_value = False
        dead.exitcode = -9
        alive.is_alive.return_value = True
        processes = {1: dead, 2: alive}

        with patch("worker.multiprocess.mark_process_dead"):
            restarted = worker.restart_dead_workers(processes, spawn=lambda worker_id: replacement)

        self.assertEqual(restarted, 1)
        self.assertIs(processes[1], replacement)
        self.assertIs(processes[2], alive)
