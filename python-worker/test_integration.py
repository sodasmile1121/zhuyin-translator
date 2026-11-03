"""Chaos tests against a real Redis. They delete the stream keys they use, so they only run when
RUN_INTEGRATION=1 (CI sets it and provides a Redis service container; REDIS_ADDR points at it)."""
import os
import unittest
from unittest.mock import patch

import pytest

import worker

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 to run against a real Redis"
)


class TestWorkerAgainstRealRedis(unittest.TestCase):

    def setUp(self):
        self.r = worker.r_text
        for key in (worker.TASK_STREAM_KEY, worker.DLQ_STREAM_KEY, worker.STREAM_KEY):
            self.r.delete(key)
        self.r.xgroup_create(worker.TASK_STREAM_KEY, worker.TASK_GROUP_NAME, id="$", mkstream=True)
        # Everything idles "long enough" immediately, and retries do not sleep.
        patches = [
            patch.object(worker, "CLAIM_MIN_IDLE_MS", 0),
            patch.object(worker, "READ_BLOCK_MS", 100),
            patch("worker.time.sleep"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        self.r.xgroup_destroy(worker.TASK_STREAM_KEY, worker.TASK_GROUP_NAME)

    def enqueue(self, job_id="job-1"):
        self.r.xadd(worker.TASK_STREAM_KEY, {"job_id": job_id, "file_path": "https://example/x.pdf"})

    def pending(self):
        return self.r.xpending(worker.TASK_STREAM_KEY, worker.TASK_GROUP_NAME)["pending"]

    def events(self):
        return [values for _, values in self.r.xrange(worker.STREAM_KEY)]

    def test_task_of_a_crashed_worker_is_taken_over_and_completed(self):
        self.enqueue()
        # worker-dead reads the task and then dies before ACKing it
        self.r.xreadgroup(worker.TASK_GROUP_NAME, "worker-dead", {worker.TASK_STREAM_KEY: ">"}, count=1)
        self.assertEqual(self.pending(), 1)

        with patch("worker.handle_single_job_logic", return_value={"s3_output_path": "outputs/job-1_translated.pdf"}):
            worker.run_once("worker-1", "0-0")

        self.assertEqual(self.pending(), 0)
        self.assertEqual(self.events(), [{"job_id": "job-1", "status": "success", "output_path": "outputs/job-1_translated.pdf"}])
        self.assertEqual(self.r.xlen(worker.DLQ_STREAM_KEY), 0)

    def test_always_failing_task_ends_in_dlq_after_max_deliveries(self):
        self.enqueue()

        with patch("worker.handle_single_job_logic", side_effect=Exception("boom")) as logic:
            for _ in range(worker.MAX_DELIVERIES + 2):
                worker.run_once("worker-1", "0-0")

        self.assertEqual(logic.call_count, worker.MAX_DELIVERIES)
        self.assertEqual(self.pending(), 0)
        dlq = [values for _, values in self.r.xrange(worker.DLQ_STREAM_KEY)]
        self.assertEqual(len(dlq), 1)
        self.assertEqual(dlq[0]["job_id"], "job-1")
        self.assertEqual(dlq[0]["error"], "boom")
        self.assertEqual(self.events()[-1]["status"], "failed")

    def test_task_that_keeps_killing_workers_is_quarantined_without_running_it(self):
        self.enqueue()
        # delivery 1, then three more deliveries that never complete (the worker died each time)
        self.r.xreadgroup(worker.TASK_GROUP_NAME, "worker-a", {worker.TASK_STREAM_KEY: ">"}, count=1)
        for consumer in ("worker-b", "worker-c"):
            self.r.xautoclaim(worker.TASK_STREAM_KEY, worker.TASK_GROUP_NAME, consumer, min_idle_time=0, count=1)

        with patch("worker.handle_single_job_logic") as logic:
            worker.run_once("worker-1", "0-0")

        logic.assert_not_called()
        self.assertEqual(self.pending(), 0)
        self.assertEqual(self.r.xlen(worker.DLQ_STREAM_KEY), 1)
        self.assertEqual(self.events()[-1]["status"], "failed")

    def test_malformed_message_is_quarantined(self):
        self.r.xadd(worker.TASK_STREAM_KEY, {"job_id": "job-only"})

        worker.run_once("worker-1", "0-0")

        self.assertEqual(self.pending(), 0)
        self.assertEqual(self.r.xlen(worker.DLQ_STREAM_KEY), 1)
