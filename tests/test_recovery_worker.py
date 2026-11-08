
from unittest.mock import Mock, call

from app.workers.recovery_worker import RecoveryWorker


def create_recovery_worker():

    processing_queue = Mock()

    worker = RecoveryWorker(
        processing_queue=processing_queue,
        visibility_timeout=30,
        poll_interval=5,
        batch_size=100
    )

    return worker, processing_queue


def test_recover_once_calls_processing_queue():

    worker, processing_queue = create_recovery_worker()

    processing_queue.recover_expired_tasks.return_value = [
        "task-1",
        "task-2"
    ]

    recovered = worker.recover_once()

    assert recovered == ["task-1", "task-2"]

    processing_queue.recover_expired_tasks.assert_called_once_with(
        visibility_timeout=30,
        batch_size=100
    )


def test_recover_once_when_no_tasks_expired():

    worker, processing_queue = create_recovery_worker()

    processing_queue.recover_expired_tasks.return_value = []

    recovered = worker.recover_once()

    assert recovered == []

    processing_queue.recover_expired_tasks.assert_called_once()


def test_worker_uses_configured_settings():

    processing_queue = Mock()

    processing_queue.recover_expired_tasks.return_value = []

    worker = RecoveryWorker(
        processing_queue=processing_queue,
        visibility_timeout=60,
        poll_interval=10,
        batch_size=50
    )

    worker.recover_once()

    processing_queue.recover_expired_tasks.assert_called_once_with(
        visibility_timeout=60,
        batch_size=50
    )

def test_stop_sets_running_false():

    worker, processing_queue = create_recovery_worker()

    worker.running = True

    worker.stop()

    assert worker.running is False


def test_run_forever_repeats_recovery(monkeypatch):

    worker, processing_queue = create_recovery_worker()

    calls = []

    def fake_recover():

        calls.append(1)

        if len(calls) == 3:
            worker.stop()

        return []

    monkeypatch.setattr(worker, "recover_once", fake_recover)

    monkeypatch.setattr(
        "app.workers.recovery_worker.time.sleep",
        lambda seconds: None
    )

    worker.run_forever()

    assert len(calls) == 3
    assert worker.running is False


def test_recovery_continues_after_temporary_error(monkeypatch):

    worker, processing_queue = create_recovery_worker()

    calls = []

    def fake_recover():

        calls.append(1)

        if len(calls) == 1:
            raise RuntimeError("Temporary Redis error")

        worker.stop()

        return []

    monkeypatch.setattr(worker, "recover_once", fake_recover)

    monkeypatch.setattr(
        "app.workers.recovery_worker.time.sleep",
        lambda seconds: None
    )

    worker.run_forever()

    assert len(calls) == 2
    assert worker.running is False
