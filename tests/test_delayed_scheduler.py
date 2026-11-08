
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

from app.core.task import TaskState
from app.workers.delayed_scheduler import DelayedScheduler


def create_scheduler():

    delayed_queue = Mock()
    ready_queue = Mock()
    task_storage = Mock()
    state_manager = Mock()

    # Match the actual ReadyQueue configuration.
    ready_queue.queue_key = "tasks:ready"
    ready_queue.sequence_key = "tasks:ready:sequence"

    state_manager.prepare_updates.return_value = {
        "state": "pending",
        "updated_at": "test-timestamp",
        "completed_at": "",
        "retry_at": "",
    }

    scheduler = DelayedScheduler(
        delayed_queue=delayed_queue,
        ready_queue=ready_queue,
        task_storage=task_storage,
        state_manager=state_manager
    )

    return (
        scheduler,
        delayed_queue,
        ready_queue,
        task_storage,
        state_manager
    )


def test_due_task_moved_to_ready_queue():

    (
        scheduler,
        delayed,
        ready,
        storage,
        state_manager
    ) = create_scheduler()

    task = Mock()
    task.state = TaskState.DELAYED
    task.priority = 8

    delayed.get_due_tasks.return_value = ["task-1"]
    delayed.get_retry_at.return_value = (
        datetime.now(timezone.utc) - timedelta(seconds=5)
    )
    storage.get_task.return_value = task

    # Simulate successful atomic Redis operation.
    delayed.transition_to_pending.return_value = 1

    result = scheduler.process_due_tasks()

    assert result == ["task-1"]

    state_manager.prepare_updates.assert_called_once_with(
        task,
        TaskState.PENDING
    )

    delayed.transition_to_pending.assert_called_once_with(
        task_id="task-1",
        updates=state_manager.prepare_updates.return_value,
        ready_queue_key="tasks:ready",
        sequence_key="tasks:ready:sequence",
    )

    # The scheduler must not perform separate operations.
    ready.enqueue.assert_not_called()
    delayed.remove_task.assert_not_called()
    state_manager.transition_task.assert_not_called()


def test_future_task_not_processed():

    (
        scheduler,
        delayed,
        ready,
        storage,
        state_manager
    ) = create_scheduler()

    task = Mock()
    task.state = TaskState.DELAYED
    task.priority = 5

    delayed.get_due_tasks.return_value = ["task-1"]
    delayed.get_retry_at.return_value = (
        datetime.now(timezone.utc) + timedelta(seconds=30)
    )
    storage.get_task.return_value = task

    result = scheduler.process_due_tasks()

    assert result == []

    delayed.transition_to_pending.assert_not_called()
    ready.enqueue.assert_not_called()
    state_manager.prepare_updates.assert_not_called()


def test_missing_task_removed_from_delayed_queue():

    (
        scheduler,
        delayed,
        ready,
        storage,
        state_manager
    ) = create_scheduler()

    delayed.get_due_tasks.return_value = ["missing-task"]
    storage.get_task.return_value = None

    result = scheduler.process_due_tasks()

    assert result == []

    delayed.remove_task.assert_called_once_with(
        "missing-task"
    )

    delayed.transition_to_pending.assert_not_called()
    ready.enqueue.assert_not_called()


def test_non_delayed_task_removed_from_delayed_queue():

    (
        scheduler,
        delayed,
        ready,
        storage,
        state_manager
    ) = create_scheduler()

    task = Mock()
    task.state = TaskState.SUCCEEDED

    delayed.get_due_tasks.return_value = ["task-1"]
    storage.get_task.return_value = task

    result = scheduler.process_due_tasks()

    assert result == []

    delayed.remove_task.assert_called_once_with(
        "task-1"
    )

    delayed.transition_to_pending.assert_not_called()
    ready.enqueue.assert_not_called()


def test_scheduler_uses_batch_size():

    (
        scheduler,
        delayed,
        ready,
        storage,
        state_manager
    ) = create_scheduler()

    scheduler.batch_size = 25

    delayed.get_due_tasks.return_value = []

    scheduler.process_due_tasks()

    delayed.get_due_tasks.assert_called_once_with(
        limit=25
    )


def test_task_priority_is_preserved():

    (
        scheduler,
        delayed,
        ready,
        storage,
        state_manager
    ) = create_scheduler()

    task = Mock()
    task.state = TaskState.DELAYED
    task.priority = 9

    delayed.get_due_tasks.return_value = ["task-1"]
    delayed.get_retry_at.return_value = (
        datetime.now(timezone.utc) - timedelta(seconds=2)
    )
    storage.get_task.return_value = task

    delayed.transition_to_pending.return_value = 1

    scheduler.process_due_tasks()

    # Priority is read directly from the task hash by the Lua script.
    assert task.priority == 9

    delayed.transition_to_pending.assert_called_once()

    call_kwargs = delayed.transition_to_pending.call_args.kwargs

    assert call_kwargs["task_id"] == "task-1"
    assert call_kwargs["ready_queue_key"] == "tasks:ready"
    assert call_kwargs["sequence_key"] == "tasks:ready:sequence"

    # No separate ready queue insertion.
    ready.enqueue.assert_not_called()
