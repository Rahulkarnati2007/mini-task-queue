
import pytest

from pydantic import ValidationError

from app.core.task import (
    Task,
    TaskCreate,
    TaskState,
    create_task
)


def test_create_task():

    request = TaskCreate(
        task_type="send_email",
        payload={"to": "rahul@example.com"},
        priority=8
    )

    task = create_task(request)

    assert task.task_type == "send_email"
    assert task.priority == 8
    assert task.state == TaskState.PENDING
    assert task.attempts == 0
    assert task.max_attempts == 3
    assert task.id is not None


def test_default_priority():

    request = TaskCreate(
        task_type="send_email"
    )

    assert request.priority == 5


def test_invalid_priority():

    with pytest.raises(ValidationError):

        TaskCreate(
            task_type="send_email",
            priority=15
        )


def test_invalid_max_attempts():

    with pytest.raises(ValidationError):

        TaskCreate(
            task_type="send_email",
            max_attempts=0
        )


def test_unique_task_ids():

    request = TaskCreate(
        task_type="send_email"
    )

    task1 = create_task(request)
    task2 = create_task(request)

    assert task1.id != task2.id


def test_task_timestamps():

    request = TaskCreate(
        task_type="send_email"
    )

    task = create_task(request)

    assert task.created_at is not None
    assert task.updated_at is not None
    assert task.started_at is None
    assert task.completed_at is None


def test_invalid_task_type():

    with pytest.raises(ValidationError):

        TaskCreate(
            task_type=""
        )
