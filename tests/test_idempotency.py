import pytest
from unittest.mock import Mock

from app.core.task import Task, TaskCreate, TaskState
from app.core.task_service import TaskService
from app.storage.task_storage import TaskStorage


def create_service():

    storage = Mock()
    ready_queue = Mock()
    dead_letter_queue = Mock()

    service = TaskService(
        task_storage=storage,
        ready_queue=ready_queue,
        dead_letter_queue=dead_letter_queue
    )

    return service, storage, ready_queue


def test_first_submission_creates_task():

    service, storage, ready_queue = create_service()

    storage.claim_idempotency_key.return_value = True

    request = TaskCreate(
        task_type="process_data",
        payload={"file": "data.csv"},
        priority=8,
        max_attempts=3,
        idempotency_key="request-123"
    )

    task = service.submit_task(request)

    assert task.state == TaskState.PENDING

    storage.claim_idempotency_key.assert_called_once_with(
        "request-123",
        task.id
    )

    storage.save_task.assert_called_once_with(task)

    ready_queue.enqueue.assert_called_once_with(
        task_id=task.id,
        priority=8
    )


def test_duplicate_submission_returns_existing_task():

    service, storage, ready_queue = create_service()

    existing_task = Task(
        id="existing-task-123",
        task_type="process_data",
        payload={"file": "data.csv"},
        priority=8,
        idempotency_key="request-123"
    )

    storage.claim_idempotency_key.return_value = False

    storage.get_idempotency_task_id.return_value = (
        "existing-task-123"
    )

    storage.get_task.return_value = existing_task

    request = TaskCreate(
        task_type="process_data",
        payload={"file": "data.csv"},
        priority=8,
        max_attempts=3,
        idempotency_key="request-123"
    )

    result = service.submit_task(request)

    assert result.id == "existing-task-123"

    storage.save_task.assert_not_called()
    ready_queue.enqueue.assert_not_called()


def test_different_keys_create_different_tasks():

    service, storage, ready_queue = create_service()

    storage.claim_idempotency_key.return_value = True

    request_1 = TaskCreate(
        task_type="process_data",
        idempotency_key="request-1"
    )

    request_2 = TaskCreate(
        task_type="process_data",
        idempotency_key="request-2"
    )

    task_1 = service.submit_task(request_1)
    task_2 = service.submit_task(request_2)

    assert task_1.id != task_2.id

    assert storage.save_task.call_count == 2
    assert ready_queue.enqueue.call_count == 2


def test_submission_without_idempotency_key():

    service, storage, ready_queue = create_service()

    request = TaskCreate(
        task_type="process_data"
    )

    task = service.submit_task(request)

    assert task.id is not None

    storage.claim_idempotency_key.assert_not_called()
    storage.save_task.assert_called_once()


def test_stale_idempotency_mapping_does_not_create_duplicate():

    service, storage, ready_queue = create_service()

    storage.claim_idempotency_key.return_value = False

    storage.get_idempotency_task_id.return_value = (
        "missing-task"
    )

    storage.get_task.return_value = None

    request = TaskCreate(
        task_type="process_data",
        idempotency_key="request-123"
    )

    with pytest.raises(RuntimeError):
        service.submit_task(request)

    storage.save_task.assert_not_called()
    ready_queue.enqueue.assert_not_called()


def test_failed_submission_releases_idempotency_key():

    service, storage, ready_queue = create_service()

    storage.claim_idempotency_key.return_value = True

    ready_queue.enqueue.side_effect = RuntimeError(
        "Redis queue insertion failed"
    )

    request = TaskCreate(
        task_type="process_data",
        idempotency_key="request-123"
    )

    with pytest.raises(RuntimeError):
        service.submit_task(request)

    task = storage.save_task.call_args[0][0]

    storage.delete_task.assert_called_once_with(task.id)

    storage.release_idempotency_key.assert_called_once_with(
        "request-123",
        task.id
    )


def test_redis_claim_idempotency_key():

    redis = Mock()
    redis.set.return_value = True

    storage = TaskStorage(redis)

    result = storage.claim_idempotency_key(
        "request-123",
        "task-123"
    )

    assert result is True

    redis.set.assert_called_once_with(
        "idempotency:request-123",
        "task-123",
        nx=True
    )


def test_redis_duplicate_key_claim_fails():

    redis = Mock()
    redis.set.return_value = None

    storage = TaskStorage(redis)

    result = storage.claim_idempotency_key(
        "request-123",
        "task-456"
    )

    assert result is False


def test_release_idempotency_key():

    redis = Mock()
    redis.eval.return_value = 1

    storage = TaskStorage(redis)

    result = storage.release_idempotency_key(
        "request-123",
        "task-123"
    )

    assert result is True
    redis.eval.assert_called_once()