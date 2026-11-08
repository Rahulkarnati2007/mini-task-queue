
import pytest
import uuid

from app.core.task import (
    TaskCreate,
    TaskState,
    create_task
)

from app.core.state_manager import (
    TaskStateManager,
    InvalidStateTransition,
    TaskNotFound
)

from app.storage.redis_client import get_redis_client
from app.storage.task_storage import TaskStorage


@pytest.fixture
def setup():

    redis_client = get_redis_client()

    assert redis_client.ping()

    storage = TaskStorage(redis_client)
    manager = TaskStateManager(storage)

    task = create_task(
        TaskCreate(
            task_type="send_email",
            payload={"to": "rahul@example.com"}
        )
    )

    storage.save_task(task)

    yield manager, storage, task

    redis_client.delete(f"task:{task.id}")


def test_pending_to_running(setup):

    manager, storage, task = setup

    updated = manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    assert updated.state == TaskState.RUNNING
    assert updated.attempts == 1
    assert updated.started_at is not None


def test_running_to_succeeded(setup):

    manager, storage, task = setup

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    updated = manager.transition_task(
        task.id,
        TaskState.SUCCEEDED
    )

    assert updated.state == TaskState.SUCCEEDED
    assert updated.completed_at is not None


def test_running_to_failed(setup):

    manager, storage, task = setup

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    updated = manager.transition_task(
        task.id,
        TaskState.FAILED,
        error="Email server unavailable"
    )

    assert updated.state == TaskState.FAILED
    assert updated.last_error == "Email server unavailable"


def test_failed_to_delayed(setup):

    manager, storage, task = setup

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    manager.transition_task(
        task.id,
        TaskState.FAILED,
        error="Network error"
    )

    updated = manager.transition_task(
        task.id,
        TaskState.DELAYED
    )

    assert updated.state == TaskState.DELAYED


def test_invalid_pending_to_succeeded(setup):

    manager, storage, task = setup

    with pytest.raises(InvalidStateTransition):

        manager.transition_task(
            task.id,
            TaskState.SUCCEEDED
        )


def test_succeeded_is_terminal(setup):

    manager, storage, task = setup

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    manager.transition_task(
        task.id,
        TaskState.SUCCEEDED
    )

    with pytest.raises(InvalidStateTransition):

        manager.transition_task(
            task.id,
            TaskState.RUNNING
        )


def test_attempt_count_increments(setup):

    manager, storage, task = setup

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    manager.transition_task(
        task.id,
        TaskState.FAILED,
        error="First attempt failed"
    )

    manager.transition_task(
        task.id,
        TaskState.DELAYED
    )

    manager.transition_task(
        task.id,
        TaskState.PENDING
    )

    updated = manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    assert updated.attempts == 2


def test_dead_task_can_be_requeued(setup):

    manager, storage, task = setup

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    manager.transition_task(
        task.id,
        TaskState.FAILED
    )

    manager.transition_task(
        task.id,
        TaskState.DEAD
    )

    updated = manager.transition_task(
        task.id,
        TaskState.PENDING
    )

    assert updated.state == TaskState.PENDING
    assert updated.completed_at is None


def test_nonexistent_task(setup):

    manager, storage, task = setup

    with pytest.raises(TaskNotFound):

        manager.transition_task(
            str(uuid.uuid4()),
            TaskState.RUNNING
        )


def test_crash_recovery_transition(setup):

    manager, storage, task = setup

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    updated = manager.transition_task(
        task.id,
        TaskState.PENDING
    )

    assert updated.state == TaskState.PENDING
    assert updated.attempts == 1
