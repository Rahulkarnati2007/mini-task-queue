
from unittest.mock import Mock

from app.core.task import TaskState
from app.workers.worker import Worker


def create_worker(task=None):

    ready_queue = Mock()
    processing_queue = Mock()
    storage = Mock()
    state_manager = Mock()

    processing_queue.reserve_task.return_value = "task-1"
    storage.get_task.return_value = task

    worker = Worker(
        worker_id="worker-test",
        ready_queue=ready_queue,
        task_storage=storage,
        state_manager=state_manager,
        processing_queue=processing_queue
    )

    return worker, processing_queue, storage, state_manager


def test_worker_reserves_and_completes_task(monkeypatch):

    task = Mock()
    task.task_type = "send_email"
    task.payload = {"to": "test@example.com"}

    worker, processing, storage, state = create_worker(task)

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        lambda task_type, payload: {"status": "success"}
    )

    result = worker.process_one()

    assert result is True

    processing.reserve_task.assert_called_once()

    state.transition_task.assert_any_call(
        "task-1",
        TaskState.RUNNING
    )

    state.transition_task.assert_any_call(
        "task-1",
        TaskState.SUCCEEDED
    )

    processing.remove_task.assert_called_once_with("task-1")


def test_worker_records_failure_and_acknowledges(monkeypatch):

    task = Mock()
    task.task_type = "send_email"
    task.payload = {}

    worker, processing, storage, state = create_worker(task)

    def failing_handler(task_type, payload):
        raise ValueError("Simulated failure")

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        failing_handler
    )

    result = worker.process_one()

    assert result is True

    state.transition_task.assert_any_call(
        "task-1",
        TaskState.RUNNING
    )

    state.transition_task.assert_any_call(
        "task-1",
        TaskState.FAILED,
        error="Simulated failure"
    )

    processing.remove_task.assert_called_once_with("task-1")


def test_missing_task_is_removed_from_processing():

    worker, processing, storage, state = create_worker(task=None)

    result = worker.process_one()

    assert result is True

    processing.remove_task.assert_called_once_with("task-1")

    state.transition_task.assert_not_called()


def test_empty_processing_queue():

    worker, processing, storage, state = create_worker()

    processing.reserve_task.return_value = None

    result = worker.process_one()

    assert result is False

    storage.get_task.assert_not_called()
    state.transition_task.assert_not_called()
    processing.remove_task.assert_not_called()
