
from datetime import datetime, timezone

from app.core.task import TaskState


class InvalidStateTransition(Exception):
    """Raised when an invalid task transition is requested."""
    pass


class TaskNotFound(Exception):
    """Raised when the requested task does not exist."""
    pass


class LeaseOwnershipError(Exception):
    """Raised when a worker no longer owns a task lease."""
    pass


class TaskStateManager:

    ALLOWED_TRANSITIONS = {
        TaskState.PENDING: {
            TaskState.RUNNING,
            TaskState.CANCELLED,
        },

        TaskState.RUNNING: {
            TaskState.SUCCEEDED,
            TaskState.FAILED,
            TaskState.PENDING,
        },

        TaskState.FAILED: {
            TaskState.DELAYED,
            TaskState.DEAD,
        },

        TaskState.DELAYED: {
            TaskState.PENDING,
            TaskState.DEAD,
        },

        TaskState.DEAD: {
            TaskState.PENDING,
        },

        TaskState.SUCCEEDED: set(),
        TaskState.CANCELLED: set(),
    }

    def __init__(self, task_storage):
        self.storage = task_storage

    # --------------------------------------------------
    # 1. Current UTC timestamp
    # --------------------------------------------------

    def get_current_time(self):
        return datetime.now(timezone.utc)

    # --------------------------------------------------
    # 2. Validate state transition
    # --------------------------------------------------

    def is_transition_allowed(
        self,
        current_state: TaskState,
        new_state: TaskState,
    ) -> bool:

        allowed_states = self.ALLOWED_TRANSITIONS.get(
            current_state,
            set(),
        )

        return new_state in allowed_states

    # --------------------------------------------------
    # 3. Prepare task updates
    # --------------------------------------------------

    def prepare_updates(
        self,
        task,
        new_state,
        error=None,
        retry_at=None,
    ):
        """
        Calculate all fields that need to change.

        Values are prepared before being sent to Redis.
        """

        current_time = self.get_current_time()

        updates = {
            "state": new_state.value,
            "updated_at": current_time.isoformat(),
        }

        if new_state == TaskState.RUNNING:

            updates["attempts"] = str(task.attempts + 1)
            updates["started_at"] = current_time.isoformat()
            updates["last_error"] = ""
            updates["retry_at"] = ""

        if new_state == TaskState.FAILED:

            updates["last_error"] = error or "Unknown error"

        if new_state == TaskState.DELAYED:

            updates["retry_at"] = (
                retry_at.isoformat() if retry_at else ""
            )

        if new_state in {
            TaskState.SUCCEEDED,
            TaskState.DEAD,
            TaskState.CANCELLED,
        }:

            updates["completed_at"] = current_time.isoformat()

        if new_state == TaskState.PENDING:

            updates["completed_at"] = ""
            updates["retry_at"] = ""

        return updates

    # --------------------------------------------------
    # 4. Atomic state transition for workers
    # --------------------------------------------------

    def atomic_transition_task(
        self,
        task_id: str,
        new_state: TaskState,
        lease_token: str,
        error=None,
        retry_at=None,
    ):
        """
        Perform a state transition with atomic lease
        ownership verification inside Redis.
        """

        task = self.storage.get_task(task_id)

        if task is None:
            raise TaskNotFound(
                f"Task {task_id} does not exist"
            )

        current_state = TaskState(task.state)

        if not self.is_transition_allowed(
            current_state,
            new_state,
        ):
            raise InvalidStateTransition(
                f"Cannot transition task from "
                f"{current_state.value} to {new_state.value}"
            )

        updates = self.prepare_updates(
            task=task,
            new_state=new_state,
            error=error,
            retry_at=retry_at,
        )

        result = self.storage.atomic_update_task(
            task_id=task_id,
            expected_state=current_state.value,
            lease_token=lease_token,
            updates=updates,
        )

        if result == 0:
            raise TaskNotFound(
                f"Task {task_id} does not exist"
            )

        if result == -1:
            raise LeaseOwnershipError(
                f"Worker no longer owns task {task_id}"
            )

        if result == -2:
            raise InvalidStateTransition(
                f"Task {task_id} state changed before "
                f"the transition to {new_state.value}"
            )

        # Return the updated task from Redis.
        return self.storage.get_task(task_id)

    # --------------------------------------------------
    # 5. Standard state transition
    # --------------------------------------------------

    def transition_task(
        self,
        task_id: str,
        new_state: TaskState,
        error: str = None,
        retry_at: datetime = None,
        lease_token: str = None,
    ):
        """
        If a lease token is provided, use atomic transition.

        Otherwise, preserve the original transition behaviour
        for API operations, schedulers, and administrative tasks.
        """

        if lease_token is not None:
            return self.atomic_transition_task(
                task_id=task_id,
                new_state=new_state,
                lease_token=lease_token,
                error=error,
                retry_at=retry_at,
            )

        # Original non-lease transition behaviour.
        task = self.storage.get_task(task_id)

        if task is None:
            raise TaskNotFound(
                f"Task {task_id} does not exist"
            )

        current_state = TaskState(task.state)

        if not self.is_transition_allowed(
            current_state,
            new_state,
        ):
            raise InvalidStateTransition(
                f"Cannot transition task from "
                f"{current_state.value} to {new_state.value}"
            )

        task.state = new_state

        current_time = self.get_current_time()
        task.updated_at = current_time

        if new_state == TaskState.RUNNING:

            task.attempts += 1
            task.started_at = current_time
            task.last_error = None
            task.retry_at = None

        if new_state == TaskState.FAILED:

            task.last_error = error or "Unknown error"

        if new_state == TaskState.DELAYED:

            task.retry_at = retry_at

        if new_state in {
            TaskState.SUCCEEDED,
            TaskState.DEAD,
            TaskState.CANCELLED,
        }:

            task.completed_at = current_time

        if new_state == TaskState.PENDING:

            task.completed_at = None
            task.retry_at = None

        self.storage.update_task(task)

        return task
