
import pytest

from datetime import datetime, timezone

from app.core.task import Task, TaskState
from app.core.state_manager import (
    TaskStateManager,
    InvalidStateTransition,
    TaskNotFound
)


# ---------------------------------------
# Fake storage for isolated testing
# ---------------------------------------

class FakeStorage:

    def __init__(self, task):

        self.task = task

    def get_task(self, task_id):

        if self.task.id == task_id:
            return self.task

        return None

    def update_task(self, task):

        self.task = task

    def delete_task(self, task_id):

        if self.task.id == task_id:
            self.task = None


# ---------------------------------------
# Helper to create a task
# ---------------------------------------

def create_test_task(state):

    return Task(
        task_type="test_task",
        payload={"message": "Testing"},
        state=state
    )


# ---------------------------------------
# 1. Test all valid transitions
# ---------------------------------------

VALID_TRANSITIONS = [

    (TaskState.PENDING, TaskState.RUNNING),

    (TaskState.PENDING, TaskState.CANCELLED),

    (TaskState.RUNNING, TaskState.SUCCEEDED),

    (TaskState.RUNNING, TaskState.FAILED),

    (TaskState.RUNNING, TaskState.PENDING),

    (TaskState.FAILED, TaskState.DELAYED),

    (TaskState.FAILED, TaskState.DEAD),

    (TaskState.DELAYED, TaskState.PENDING),

    (TaskState.DELAYED, TaskState.DEAD),

    (TaskState.DEAD, TaskState.PENDING)
]


@pytest.mark.parametrize(
    "current_state,new_state",
    VALID_TRANSITIONS
)
def test_valid_transitions(current_state, new_state):

    task = create_test_task(current_state)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    updated_task = manager.transition_task(
        task.id,
        new_state
    )

    assert updated_task.state == new_state


# ---------------------------------------
# 2. Test invalid transitions
# ---------------------------------------

INVALID_TRANSITIONS = [

    (TaskState.PENDING, TaskState.SUCCEEDED),

    (TaskState.PENDING, TaskState.FAILED),

    (TaskState.RUNNING, TaskState.CANCELLED),

    (TaskState.SUCCEEDED, TaskState.RUNNING),

    (TaskState.SUCCEEDED, TaskState.PENDING),

    (TaskState.CANCELLED, TaskState.RUNNING),

    (TaskState.CANCELLED, TaskState.PENDING),

    (TaskState.DEAD, TaskState.RUNNING),

    (TaskState.FAILED, TaskState.SUCCEEDED)
]


@pytest.mark.parametrize(
    "current_state,new_state",
    INVALID_TRANSITIONS
)
def test_invalid_transitions(current_state, new_state):

    task = create_test_task(current_state)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    with pytest.raises(InvalidStateTransition):

        manager.transition_task(
            task.id,
            new_state
        )


# ---------------------------------------
# 3. Test execution attempt counting
# ---------------------------------------

def test_running_transition_increments_attempts():

    task = create_test_task(TaskState.PENDING)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    # First execution
    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    assert task.attempts == 1

    # First execution fails
    manager.transition_task(
        task.id,
        TaskState.FAILED,
        error="Temporary failure"
    )

    # Move to delayed state before retrying
    manager.transition_task(
        task.id,
        TaskState.DELAYED
    )

    # Retry becomes eligible
    manager.transition_task(
        task.id,
        TaskState.PENDING
    )

    # Second execution
    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    assert task.attempts == 2



# ---------------------------------------
# 4. Test execution timestamps
# ---------------------------------------

def test_running_sets_started_at():

    task = create_test_task(TaskState.PENDING)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    manager.transition_task(
        task.id,
        TaskState.RUNNING
    )

    assert task.started_at is not None

    assert task.updated_at is not None


# ---------------------------------------
# 5. Test failure recording
# ---------------------------------------

def test_failed_transition_records_error():

    task = create_test_task(TaskState.RUNNING)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    manager.transition_task(
        task.id,
        TaskState.FAILED,
        error="Database connection failed"
    )

    assert task.state == TaskState.FAILED

    assert task.last_error == (
        "Database connection failed"
    )


# ---------------------------------------
# 6. Test cancellation timestamp
# ---------------------------------------

def test_cancelled_task_gets_completion_time():

    task = create_test_task(TaskState.PENDING)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    manager.transition_task(
        task.id,
        TaskState.CANCELLED
    )

    assert task.state == TaskState.CANCELLED

    assert task.completed_at is not None


# ---------------------------------------
# 7. Test successful completion timestamp
# ---------------------------------------

def test_succeeded_task_gets_completion_time():

    task = create_test_task(TaskState.RUNNING)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    manager.transition_task(
        task.id,
        TaskState.SUCCEEDED
    )

    assert task.completed_at is not None


# ---------------------------------------
# 8. Test nonexistent task
# ---------------------------------------

def test_transition_nonexistent_task():

    task = create_test_task(TaskState.PENDING)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    with pytest.raises(TaskNotFound):

        manager.transition_task(
            "nonexistent-task-id",
            TaskState.RUNNING
        )


# ---------------------------------------
# 9. Test completion time cleared on requeue
# ---------------------------------------

def test_pending_transition_clears_completion_time():

    task = create_test_task(TaskState.DELAYED)

    # Simulate an old completion timestamp
    task.completed_at = datetime.now(timezone.utc)

    storage = FakeStorage(task)

    manager = TaskStateManager(storage)

    manager.transition_task(
        task.id,
        TaskState.PENDING
    )

    assert task.state == TaskState.PENDING

    assert task.completed_at is None
