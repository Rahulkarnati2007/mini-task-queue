
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.core.task import TaskState
from app.workers.worker import Worker


# ---------------------------------------
# Fake components for testing
# ---------------------------------------

@pytest.fixture
def worker_components():

    task_id = "test-task-001"

    # Simulate a task
    task = SimpleNamespace(
        id=task_id,
        task_type="send_email",
        payload={
            "to": "rahul@example.com",
            "subject": "Test Email"
        }
    )

    # Simulate Redis ready queue
    queue = Mock()
    queue.dequeue.return_value = task_id

    # Simulate task storage
    storage = Mock()
    storage.get_task.return_value = task

    # Simulate state manager
    state_manager = Mock()

    worker = Worker(
        worker_id="worker-1",
        ready_queue=queue,
        task_storage=storage,
        state_manager=state_manager
    )

    return worker, queue, storage, state_manager, task


# ---------------------------------------
# 1. Successful task execution
# ---------------------------------------

def test_worker_executes_task_successfully(
    worker_components,
    monkeypatch
):

    worker, queue, storage, manager, task = worker_components

    # Mock the handler so no actual email is sent
    def fake_handler(task_type, payload):

        assert task_type == "send_email"

        assert payload["to"] == "rahul@example.com"

        return {"status": "success"}

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        fake_handler
    )

    result = worker.process_one()

    # Task was found and processed
    assert result is True

    # Verify dequeue was called
    queue.dequeue.assert_called_once()

    # Verify task was retrieved
    storage.get_task.assert_called_once_with(task.id)

    # Verify state transitions
    assert manager.transition_task.call_count == 2

    calls = manager.transition_task.call_args_list

    assert calls[0].args == (
        task.id,
        TaskState.RUNNING
    )

    assert calls[1].args == (
        task.id,
        TaskState.SUCCEEDED
    )


# ---------------------------------------
# 2. Failed task execution
# ---------------------------------------

def test_worker_handles_task_failure(
    worker_components,
    monkeypatch
):

    worker, queue, storage, manager, task = worker_components

    # Simulate a handler failure
    def failing_handler(task_type, payload):

        raise ValueError("Simulated task failure")

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        failing_handler
    )

    result = worker.process_one()

    assert result is True

    calls = manager.transition_task.call_args_list

    # First transition must be RUNNING
    assert calls[0].args == (
        task.id,
        TaskState.RUNNING
    )

    # Second transition must be FAILED
    assert calls[1].args == (
        task.id,
        TaskState.FAILED
    )

    # Verify error message was recorded
    assert calls[1].kwargs["error"] == (
        "Simulated task failure"
    )


# ---------------------------------------
# 3. Empty queue handling
# ---------------------------------------

def test_worker_handles_empty_queue(worker_components):

    worker, queue, storage, manager, task = worker_components

    queue.dequeue.return_value = None

    result = worker.process_one()

    assert result is False

    # No task should be retrieved
    storage.get_task.assert_not_called()

    # No state transition should happen
    manager.transition_task.assert_not_called()


# ---------------------------------------
# 4. Missing task handling
# ---------------------------------------

def test_worker_handles_missing_task(worker_components):

    worker, queue, storage, manager, task = worker_components

    storage.get_task.return_value = None

    result = worker.process_one()

    assert result is True

    manager.transition_task.assert_not_called()


# ---------------------------------------
# 5. Worker stops correctly
# ---------------------------------------

def test_worker_stop():

    worker = Worker(
        worker_id="worker-1",
        ready_queue=Mock(),
        task_storage=Mock(),
        state_manager=Mock()
    )

    worker.running = True

    worker.stop()

    assert worker.running is False
