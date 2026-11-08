
import threading
from unittest.mock import Mock

from app.workers.worker import Worker


class FakeProcessingQueue:

    def __init__(self, renewal_result=True):
        self.renewal_result = renewal_result
        self.renewal_called = threading.Event()
        self.renewal_count = 0

    def renew_lease(self, task_id, lease_token):
        self.renewal_count += 1
        self.renewal_called.set()
        return self.renewal_result


def create_worker(processing_queue, heartbeat_interval=0.02):

    return Worker(
        worker_id="worker-1",
        ready_queue=Mock(),
        task_storage=Mock(),
        state_manager=Mock(),
        processing_queue=processing_queue,
        heartbeat_interval=heartbeat_interval,
    )


def test_heartbeat_renews_lease():

    processing = FakeProcessingQueue(renewal_result=True)
    worker = create_worker(processing)

    thread, stop_event, lost_event = worker.start_heartbeat(
        "task-123",
        "lease-abc",
    )

    try:
        assert processing.renewal_called.wait(timeout=1)
        assert processing.renewal_count >= 1
        assert not lost_event.is_set()

    finally:
        worker.stop_heartbeat(thread, stop_event)

    assert not thread.is_alive()


def test_failed_heartbeat_marks_lease_as_lost():

    processing = FakeProcessingQueue(renewal_result=False)
    worker = create_worker(processing)

    thread, stop_event, lost_event = worker.start_heartbeat(
        "task-123",
        "lease-abc",
    )

    try:
        assert lost_event.wait(timeout=1)
        assert processing.renewal_count == 1

    finally:
        worker.stop_heartbeat(thread, stop_event)

    assert not thread.is_alive()


def test_heartbeat_stops_when_task_finishes():

    processing = FakeProcessingQueue(renewal_result=True)
    worker = create_worker(processing)

    thread, stop_event, lost_event = worker.start_heartbeat(
        "task-123",
        "lease-abc",
    )

    assert processing.renewal_called.wait(timeout=1)

    worker.stop_heartbeat(thread, stop_event)

    count_after_stop = processing.renewal_count

    assert not thread.is_alive()
    assert stop_event.is_set()

    # No additional renewals should occur after stopping.
    assert processing.renewal_count == count_after_stop
