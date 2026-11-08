import signal
import threading
from unittest.mock import Mock

import pytest

from app.workers.worker import Worker


def create_worker():
    return Worker(
        worker_id="worker-signal-test",
        ready_queue=Mock(),
        task_storage=Mock(),
        state_manager=Mock(),
    )


def test_sigint_triggers_graceful_worker_shutdown(monkeypatch):
    worker = create_worker()

    previous_handler = Mock(name="previous_sigint_handler")
    installed_handlers = {}
    signal_calls = []

    monkeypatch.setattr(signal, "getsignal", lambda signum: previous_handler)

    def fake_signal(signum, handler):
        signal_calls.append((signum, handler))
        if handler is not previous_handler:
            installed_handlers[signum] = handler
        return previous_handler

    monkeypatch.setattr(signal, "signal", fake_signal)

    def simulate_sigint():
        handler = installed_handlers[signal.SIGINT]
        handler(signal.SIGINT, None)
        return False

    worker.process_one = simulate_sigint
    worker.run_forever(poll_interval=0.01)

    assert worker.running is False
    assert worker.shutdown_event.is_set()
    assert signal.SIGINT in installed_handlers
    assert installed_handlers[signal.SIGINT] is not previous_handler

    # The original handler is restored during shutdown.
    assert (signal.SIGINT, previous_handler) in signal_calls


@pytest.mark.skipif(
    not hasattr(signal, "SIGTERM"),
    reason="SIGTERM is not available on this platform",
)
def test_sigterm_triggers_graceful_worker_shutdown(monkeypatch):
    worker = create_worker()

    previous_handler = Mock(name="previous_sigterm_handler")
    installed_handlers = {}
    signal_calls = []

    monkeypatch.setattr(signal, "getsignal", lambda signum: previous_handler)

    def fake_signal(signum, handler):
        signal_calls.append((signum, handler))
        if handler is not previous_handler:
            installed_handlers[signum] = handler
        return previous_handler

    monkeypatch.setattr(signal, "signal", fake_signal)

    def simulate_sigterm():
        handler = installed_handlers[signal.SIGTERM]
        handler(signal.SIGTERM, None)
        return False

    worker.process_one = simulate_sigterm
    worker.run_forever(poll_interval=0.01)

    assert worker.running is False
    assert worker.shutdown_event.is_set()
    assert signal.SIGTERM in installed_handlers
    assert (signal.SIGTERM, previous_handler) in signal_calls


def test_signal_handlers_are_not_installed_from_background_thread(monkeypatch):
    worker = create_worker()
    signal_install_attempts = []
    run_finished = threading.Event()

    def fake_signal(signum, handler):
        signal_install_attempts.append(signum)

    monkeypatch.setattr(signal, "signal", fake_signal)

    def run_worker():
        try:
            worker.process_one = lambda: False
            worker.run_forever(poll_interval=10)
        finally:
            run_finished.set()

    thread = threading.Thread(target=run_worker, daemon=True)
    thread.start()

    try:
        # Wait until run_forever has started and is waiting for work.
        assert run_finished.wait(timeout=0.05) is False
        worker.stop()
        thread.join(timeout=1)

        assert not thread.is_alive()
        assert signal_install_attempts == []
        assert worker.shutdown_event.is_set()
    finally:
        worker.stop()
        thread.join(timeout=1)
