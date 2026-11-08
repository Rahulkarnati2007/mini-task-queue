import signal
from unittest.mock import Mock

from app.workers.worker import Worker


def create_worker():
    return Worker(
        worker_id="worker-restart-test",
        ready_queue=Mock(),
        task_storage=Mock(),
        state_manager=Mock(),
    )


def test_stop_is_safe_to_call_multiple_times():
    worker = create_worker()
    worker.running = True

    worker.stop()
    worker.stop()
    worker.stop()

    assert worker.running is False
    assert worker.shutdown_event.is_set()


def test_worker_can_start_again_after_graceful_shutdown(monkeypatch):
    worker = create_worker()

    # Keep this test independent of the machine's real signal handlers.
    original_handler = signal.getsignal(signal.SIGINT)
    monkeypatch.setattr(signal, "getsignal", lambda signum: original_handler)
    monkeypatch.setattr(signal, "signal", lambda signum, handler: original_handler)

    run_count = 0

    def process_one_then_stop():
        nonlocal run_count
        run_count += 1
        worker.stop()
        return False

    worker.process_one = process_one_then_stop

    # First lifecycle.
    worker.run_forever(poll_interval=0.01)

    assert run_count == 1
    assert worker.running is False
    assert worker.shutdown_event.is_set()

    # Second lifecycle: run_forever should clear the previous shutdown event.
    worker.run_forever(poll_interval=0.01)

    assert run_count == 2
    assert worker.running is False
    assert worker.shutdown_event.is_set()
