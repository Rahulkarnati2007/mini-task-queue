
import logging
import time
from datetime import datetime, timezone

from app.core.task import TaskState


logger = logging.getLogger(__name__)


class DelayedScheduler:

    def __init__(
        self,
        delayed_queue,
        ready_queue,
        task_storage,
        state_manager,
        batch_size=100
    ):
        self.delayed_queue = delayed_queue
        self.ready_queue = ready_queue
        self.storage = task_storage
        self.state_manager = state_manager
        self.batch_size = batch_size
        self.running = False

    def process_due_tasks(self):

        due_tasks = self.delayed_queue.get_due_tasks(
            limit=self.batch_size
        )

        processed = []

        for task_id in due_tasks:

            try:
                task = self.storage.get_task(task_id)

                if task is None:
                    logger.error(
                        "Task %s not found. Removing stale delayed entry.",
                        task_id
                    )
                    self.delayed_queue.remove_task(task_id)
                    continue

                if task.state != TaskState.DELAYED:
                    logger.warning(
                        "Task %s is not DELAYED. Removing stale entry.",
                        task_id
                    )
                    self.delayed_queue.remove_task(task_id)
                    continue

                retry_at = self.delayed_queue.get_retry_at(task_id)

                if retry_at is None:
                    continue

                if retry_at > datetime.now(timezone.utc):
                    continue

                # Prepare the PENDING state updates.
                updates = self.state_manager.prepare_updates(
                    task,
                    TaskState.PENDING
                )

                # Perform the entire transition atomically in Redis.
                result = self.delayed_queue.transition_to_pending(
                    task_id=task_id,
                    updates=updates,
                    ready_queue_key=self.ready_queue.queue_key,
                    sequence_key=self.ready_queue.sequence_key,
                )

                if result == 1:
                    processed.append(task_id)

                    logger.info(
                        "Task %s atomically moved from DELAYED to PENDING",
                        task_id
                    )

                elif result == 0:
                    logger.error(
                        "Task %s disappeared during promotion.",
                        task_id
                    )
                    self.delayed_queue.remove_task(task_id)

                elif result == -1:
                    logger.warning(
                        "Task %s is no longer DELAYED. Removing stale entry.",
                        task_id
                    )
                    self.delayed_queue.remove_task(task_id)

                elif result == -2:
                    logger.debug(
                        "Task %s is not due yet.",
                        task_id
                    )

                elif result == -3:
                    logger.warning(
                        "Task %s has no delayed queue entry.",
                        task_id
                    )

                elif result == -4:
                    logger.error(
                        "Task %s has an invalid priority.",
                        task_id
                    )

            except Exception:
                logger.exception(
                    "Failed to schedule delayed task %s",
                    task_id
                )

        return processed

    def run_forever(self, poll_interval=1):

        self.running = True

        logger.info("Delayed scheduler started")

        try:
            while self.running:

                self.process_due_tasks()

                if self.running:
                    time.sleep(poll_interval)

        except KeyboardInterrupt:
            logger.info("Delayed scheduler interrupted")

        finally:
            self.running = False
            logger.info("Delayed scheduler stopped")

    def stop(self):

        self.running = False
        logger.info("Stopping delayed scheduler")
