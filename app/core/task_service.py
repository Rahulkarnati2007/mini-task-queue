from app.core.task import (
    TaskCreate,
    TaskState,
    create_task
)

from app.core.state_manager import (
    TaskStateManager,
    TaskNotFound,
    InvalidStateTransition
)

from app.storage.task_storage import TaskStorage
from app.storage.ready_queue import ReadyQueue
from app.storage.dead_letter_queue import DeadLetterQueue


class TaskService:

    def __init__(
        self,
        task_storage: TaskStorage,
        ready_queue: ReadyQueue,
        dead_letter_queue: DeadLetterQueue = None
    ):

        self.storage = task_storage
        self.queue = ready_queue
        self.dead_letter_queue = dead_letter_queue

        self.state_manager = TaskStateManager(
            self.storage
        )

    # ---------------------------------------
    # 1. Submit task with idempotency
    # ---------------------------------------

    def submit_task(self, request: TaskCreate):

        task = create_task(request)
        idempotency_key = request.idempotency_key

        # If a key is supplied, try to claim it.
        if idempotency_key:

            claimed = self.storage.claim_idempotency_key(
                idempotency_key,
                task.id
            )

            # Another request already owns this key.
            if not claimed:

                existing_task_id = (
                    self.storage.get_idempotency_task_id(
                        idempotency_key
                    )
                )

                if existing_task_id:

                    existing_task = self.storage.get_task(
                        existing_task_id
                    )

                    if existing_task is not None:
                        return existing_task

                # Mapping exists but task isn't available yet.
                # Don't create a second task.
                raise RuntimeError(
                    "A submission with this idempotency key "
                    "is already being processed. Please retry."
                )

        try:

            self.storage.save_task(task)

            self.queue.enqueue(
                task_id=task.id,
                priority=task.priority
            )

        except Exception:

            self.queue.remove_task(task.id)
            self.storage.delete_task(task.id)

            # Release only the mapping created by this request.
            if idempotency_key:
                self.storage.release_idempotency_key(
                    idempotency_key,
                    task.id
                )

            raise

        return task

    # ---------------------------------------
    # 2. Retrieve task by ID
    # ---------------------------------------

    def get_task(self, task_id: str):
        return self.storage.get_task(task_id)

    # ---------------------------------------
    # 3. Get all tasks in ready queue
    # ---------------------------------------

    def get_ready_tasks(self):

        task_ids = self.queue.get_all_tasks()
        tasks = []

        for task_id in task_ids:

            task = self.storage.get_task(task_id)

            if task is not None:
                tasks.append(task)

        return tasks

    # ---------------------------------------
    # 4. Get ready queue size
    # ---------------------------------------

    def get_queue_size(self):
        return self.queue.get_queue_size()

    # ---------------------------------------
    # 5. Cancel a pending task
    # ---------------------------------------

    def cancel_task(self, task_id: str):

        task = self.storage.get_task(task_id)

        if task is None:
            raise TaskNotFound(
                f"Task {task_id} does not exist"
            )

        if task.state != TaskState.PENDING:
            raise InvalidStateTransition(
                f"Cannot cancel task in {task.state.value} state"
            )

        removed = self.queue.remove_task(task_id)

        if not removed:
            raise InvalidStateTransition(
                "Task is no longer in the ready queue "
                "and cannot be cancelled"
            )

        return self.state_manager.transition_task(
            task_id,
            TaskState.CANCELLED
        )

    # ---------------------------------------
    # 6. Get tasks in Dead Letter Queue
    # ---------------------------------------

    def get_dead_tasks(self):

        if self.dead_letter_queue is None:
            return []

        task_ids = self.dead_letter_queue.get_all_tasks()
        tasks = []

        for task_id in task_ids:

            task = self.storage.get_task(task_id)

            if task is not None and task.state == TaskState.DEAD:
                tasks.append(task)

        return tasks

    # ---------------------------------------
    # 7. Manually requeue a dead task
    # ---------------------------------------

    def requeue_dead_task(self, task_id: str):

        if self.dead_letter_queue is None:
            raise InvalidStateTransition(
                "Dead Letter Queue is not configured"
            )

        task = self.storage.get_task(task_id)

        if task is None:
            raise TaskNotFound(
                f"Task {task_id} does not exist"
            )

        if task.state != TaskState.DEAD:
            raise InvalidStateTransition(
                f"Cannot requeue task in {task.state.value} state"
            )

        if not self.dead_letter_queue.contains(task_id):
            raise InvalidStateTransition(
                "Task is not present in the Dead Letter Queue"
            )

        task = self.state_manager.transition_task(
            task_id,
            TaskState.PENDING
        )

        task.attempts = 0
        task.last_error = None

        self.storage.update_task(task)

        self.queue.enqueue(
            task_id=task.id,
            priority=task.priority
        )

        self.dead_letter_queue.remove_task(task_id)

        return task