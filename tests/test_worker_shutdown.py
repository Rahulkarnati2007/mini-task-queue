import threading
import time
from unittest.mock import Mock

from app.workers.worker import Worker


def create_worker():
    return Worker(
        worker_id="worker-shutdown-test",
        ready_queue=Mock(),
        task_storage=Mock(),
        state_manager=Mock(),
    )


def test_stop_sets_shutdown_event_and_running_false():

    worker = create_worker()
    worker.running = True

    worker.stop()

    assert worker.running is False
    assert worker.shutdown_event.is_set()


def test_process_one_does_not_reserve_task_after_shutdown():

    worker = create_worker()
    worker.reserve_task = Mock()

    worker.stop()

    result = worker.process_one()

    assert result is False
    worker.reserve_task.assert_not_called()


def test_run_forever_exits_when_stop_is_called():

    worker = create_worker()
    entered_loop = threading.Event()

    def no_task_available():
        entered_loop.set()
        return False

    worker.process_one = no_task_available

    thread = threading.Thread(
        target=worker.run_forever,
        kwargs={"poll_interval": 10},
        daemon=True,
    )

    thread.start()

    try:
        assert entered_loop.wait(timeout=1)

        worker.stop()
        thread.join(timeout=1)

        assert not thread.is_alive()
        assert worker.running is False
        assert worker.shutdown_event.is_set()

    finally:
        worker.stop()
        thread.join(timeout=1)


def test_worker_finishes_current_task_before_exiting():

    worker = create_worker()

    task_started = threading.Event()
    allow_task_to_finish = threading.Event()
    task_finished = threading.Event()

    def process_current_task():
        task_started.set()
        allow_task_to_finish.wait(timeout=2)
        task_finished.set()
        return True

    worker.process_one = process_current_task

    thread = threading.Thread(
        target=worker.run_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )

    thread.start()

    try:
        assert task_started.wait(timeout=1)

        worker.stop()

        # The current task is allowed to finish.
        assert not task_finished.is_set()

        allow_task_to_finish.set()
        thread.join(timeout=1)

        assert task_finished.is_set()
        assert not thread.is_alive()

    finally:
        allow_task_to_finish.set()
        worker.stop()
        thread.join(timeout=1)
