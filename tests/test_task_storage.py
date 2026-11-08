
import pytest

from app.core.task import (
    TaskCreate,
    TaskState,
    create_task
)

from app.storage.redis_client import get_redis_client
from app.storage.task_storage import TaskStorage


@pytest.fixture
def storage():

    redis_client = get_redis_client()

    # Verify Redis is available
    assert redis_client.ping()

    task_storage = TaskStorage(redis_client)

    yield task_storage

    # Clean up test task records
    # Each test uses its own unique task ID.
    # Individual tests delete their own records.


def test_save_and_retrieve_task(storage):

    request = TaskCreate(
        task_type="send_email",
        payload={"to": "rahul@example.com"},
        priority=8
    )

    task = create_task(request)

    storage.save_task(task)

    retrieved_task = storage.get_task(task.id)

    assert retrieved_task is not None
    assert retrieved_task.id == task.id
    assert retrieved_task.task_type == "send_email"
    assert retrieved_task.payload == {
        "to": "rahul@example.com"
    }
    assert retrieved_task.priority == 8

    storage.delete_task(task.id)


def test_task_state_is_preserved(storage):

    request = TaskCreate(
        task_type="send_email"
    )

    task = create_task(request)

    task.state = TaskState.RUNNING

    storage.save_task(task)

    retrieved_task = storage.get_task(task.id)

    assert retrieved_task.state == TaskState.RUNNING

    storage.delete_task(task.id)


def test_task_attempts_are_preserved(storage):

    request = TaskCreate(
        task_type="send_email"
    )

    task = create_task(request)

    task.attempts = 2

    storage.save_task(task)

    retrieved_task = storage.get_task(task.id)

    assert retrieved_task.attempts == 2

    storage.delete_task(task.id)


def test_update_task(storage):

    request = TaskCreate(
        task_type="send_email"
    )

    task = create_task(request)

    storage.save_task(task)

    task.state = TaskState.SUCCEEDED
    task.completed_at = task.created_at

    result = storage.update_task(task)

    retrieved_task = storage.get_task(task.id)

    assert result is True
    assert retrieved_task.state == TaskState.SUCCEEDED
    assert retrieved_task.completed_at is not None

    storage.delete_task(task.id)


def test_update_nonexistent_task(storage):

    request = TaskCreate(
        task_type="send_email"
    )

    task = create_task(request)

    assert storage.update_task(task) is False


def test_delete_task(storage):

    request = TaskCreate(
        task_type="send_email"
    )

    task = create_task(request)

    storage.save_task(task)

    result = storage.delete_task(task.id)

    assert result is True
    assert storage.get_task(task.id) is None


def test_get_nonexistent_task(storage):

    request = TaskCreate(
        task_type="send_email"
    )

    task = create_task(request)

    assert storage.get_task(task.id) is None
