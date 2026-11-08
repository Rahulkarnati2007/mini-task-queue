
import time
import logging
import threading
import signal

from datetime import datetime, timezone, timedelta

from app.core.task import TaskState
from app.workers.handlers import execute_handler
from app.workers.retry_policy import RetryPolicy


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


class Worker:

    def __init__(
        self,
        worker_id: str,
        ready_queue,
        task_storage,
        state_manager,
        processing_queue=None,
        delayed_queue=None,
        retry_policy=None,
        dead_letter_queue=None,
        heartbeat_interval=10,
        metrics=None,
        worker_heartbeat_interval=5,
    ):
        self.worker_id = worker_id
        self.queue = ready_queue
        self.storage = task_storage
        self.state_manager = state_manager
        self.processing_queue = processing_queue
        self.delayed_queue = delayed_queue
        self.dead_letter_queue = dead_letter_queue

        self.retry_policy = retry_policy or RetryPolicy()
        self.heartbeat_interval = heartbeat_interval
        self.metrics = metrics
        self.worker_heartbeat_interval = worker_heartbeat_interval
        self.worker_heartbeat_stop_event = threading.Event()
        self.worker_heartbeat_thread = None
        self.running = False
        self.shutdown_event = threading.Event()

    def update_metrics_status(self, status):
        """Update worker status without allowing metrics failures to stop work."""
        if self.metrics is None:
            return

        try:
            self.metrics.set_status(self.worker_id, status)
        except Exception:
            logger.exception(
                "Could not update metrics status for worker %s",
                self.worker_id,
            )

    def record_execution_metrics(self, succeeded, duration):
        """Record one completed handler execution."""
        if self.metrics is None:
            return

        try:
            self.metrics.record_execution(
                self.worker_id,
                succeeded=succeeded,
                duration=duration,
            )
        except Exception:
            logger.exception(
                "Could not record execution metrics for worker %s",
                self.worker_id,
            )


    # --------------------------------------------------
    # Worker-level heartbeat (independent of task leases)
    # --------------------------------------------------

    def record_worker_heartbeat(self):
        """Persist a heartbeat showing that this worker process is alive."""
        if self.metrics is None:
            return

        try:
            self.metrics.record_heartbeat(self.worker_id)
        except Exception:
            # A metrics outage must not stop task processing.
            logger.exception(
                "Could not record heartbeat for worker %s",
                self.worker_id,
            )

    def worker_heartbeat_loop(self):
        """Periodically report liveness, even while the queue is empty."""
        while not self.worker_heartbeat_stop_event.wait(
            self.worker_heartbeat_interval
        ):
            self.record_worker_heartbeat()

    def start_worker_heartbeat(self):
        """Write an immediate heartbeat and start periodic heartbeats."""
        if self.metrics is None:
            return

        self.worker_heartbeat_stop_event.clear()
        self.record_worker_heartbeat()

        self.worker_heartbeat_thread = threading.Thread(
            target=self.worker_heartbeat_loop,
            daemon=True,
            name=f"worker-heartbeat-{self.worker_id}",
        )
        self.worker_heartbeat_thread.start()

    def stop_worker_heartbeat(self):
        """Stop the periodic worker heartbeat thread cleanly."""
        self.worker_heartbeat_stop_event.set()

        if self.worker_heartbeat_thread is not None:
            self.worker_heartbeat_thread.join(
                timeout=max(self.worker_heartbeat_interval + 1, 2)
            )
            self.worker_heartbeat_thread = None

    # --------------------------------------------------
    # 1. Reserve a task
    # --------------------------------------------------

    def reserve_task(self):

        if self.processing_queue is not None:

            lease_method = getattr(
                type(self.processing_queue),
                "reserve_task_with_lease",
                None,
            )

            if callable(lease_method):
                return self.processing_queue.reserve_task_with_lease(
                    self.worker_id
                )

            # Backward compatibility.
            task_id = self.processing_queue.reserve_task()

            if task_id is None:
                return None

            return task_id, None

        task_id = self.queue.dequeue()

        if task_id is None:
            return None

        return task_id, None

    # --------------------------------------------------
    # 2. Centralized state transition
    # --------------------------------------------------

    def transition_task(
        self,
        task_id,
        new_state,
        lease_token=None,
        error=None,
        retry_at=None,
    ):
        """
        Use atomic state transitions whenever a valid
        worker lease token is available.
        """

        kwargs = {}

        if lease_token is not None:
            kwargs["lease_token"] = lease_token

        if error is not None:
            kwargs["error"] = error

        if retry_at is not None:
            kwargs["retry_at"] = retry_at

        return self.state_manager.transition_task(
            task_id,
            new_state,
            **kwargs,
        )

    # --------------------------------------------------
    # 3. Check lease ownership
    # --------------------------------------------------

    def owns_task(self, task_id, lease_token):

        if self.processing_queue is None or lease_token is None:
            return True

        return self.processing_queue.is_lease_owner(
            task_id,
            lease_token,
        )

    # --------------------------------------------------
    # 4. Acknowledge task safely
    # --------------------------------------------------

    def acknowledge_task(self, task_id, lease_token=None):

        if self.processing_queue is None:
            return True

        if lease_token is None:
            return self.processing_queue.remove_task(task_id)

        acknowledged = self.processing_queue.acknowledge_task(
            task_id,
            lease_token,
        )

        if not acknowledged:
            logger.warning(
                "Worker %s could not acknowledge task %s: "
                "lease is no longer owned",
                self.worker_id,
                task_id,
            )

        return acknowledged

    # --------------------------------------------------
    # 5. Heartbeat loop
    # --------------------------------------------------

    def heartbeat_loop(
        self,
        task_id,
        lease_token,
        stop_event,
        lease_lost_event,
    ):

        while not stop_event.wait(self.heartbeat_interval):

            try:
                renewed = self.processing_queue.renew_lease(
                    task_id,
                    lease_token,
                )

                if not renewed:
                    logger.warning(
                        "Worker %s lost lease for task %s",
                        self.worker_id,
                        task_id,
                    )

                    lease_lost_event.set()
                    return

                logger.debug(
                    "Worker %s renewed lease for task %s",
                    self.worker_id,
                    task_id,
                )

            except Exception:

                logger.exception(
                    "Heartbeat failed for task %s",
                    task_id,
                )

                lease_lost_event.set()
                return

    # --------------------------------------------------
    # 6. Start heartbeat
    # --------------------------------------------------

    def start_heartbeat(self, task_id, lease_token):

        if (
            self.processing_queue is None
            or lease_token is None
        ):
            return None, None, None

        stop_event = threading.Event()
        lease_lost_event = threading.Event()

        heartbeat_thread = threading.Thread(
            target=self.heartbeat_loop,
            args=(
                task_id,
                lease_token,
                stop_event,
                lease_lost_event,
            ),
            daemon=True,
            name=f"heartbeat-{self.worker_id}",
        )

        heartbeat_thread.start()

        return (
            heartbeat_thread,
            stop_event,
            lease_lost_event,
        )

    # --------------------------------------------------
    # 7. Stop heartbeat
    # --------------------------------------------------

    def stop_heartbeat(
        self,
        heartbeat_thread,
        stop_event,
    ):

        if heartbeat_thread is None:
            return

        stop_event.set()

        heartbeat_thread.join(
            timeout=max(self.heartbeat_interval + 1, 2)
        )

    # --------------------------------------------------
    # 8. Retry or move task to DLQ
    # --------------------------------------------------

    def schedule_retry(
        self,
        task_id,
        failed_task,
        lease_token=None,
    ):

        # Check whether all attempts have been exhausted.
        if failed_task.attempts >= failed_task.max_attempts:

            if self.dead_letter_queue is None:

                logger.error(
                    "Task %s exhausted attempts, "
                    "but DLQ is not configured",
                    task_id,
                )

                return False

            # Atomic FAILED -> DEAD transition.
            self.transition_task(
                task_id,
                TaskState.DEAD,
                lease_token=lease_token,
            )

            self.dead_letter_queue.add_task(task_id)

            logger.error(
                "Task %s moved to Dead Letter Queue "
                "after %s attempts",
                task_id,
                failed_task.attempts,
            )

            return False

        if self.delayed_queue is None:

            logger.warning(
                "Delayed queue not configured. "
                "Task %s remains FAILED.",
                task_id,
            )

            return False

        delay = self.retry_policy.calculate_delay(
            failed_task.attempts
        )

        retry_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=delay)
        )

        # Atomic FAILED -> DELAYED transition.
        self.transition_task(
            task_id,
            TaskState.DELAYED,
            lease_token=lease_token,
            retry_at=retry_at,
        )

        self.delayed_queue.enqueue(
            task_id,
            retry_at,
        )

        logger.warning(
            "Task %s scheduled for retry in %.1f seconds",
            task_id,
            delay,
        )

        return True

    # --------------------------------------------------
    # 9. Process one task
    # --------------------------------------------------

    def process_one(self):

        # Do not reserve another task after shutdown begins.
        if self.shutdown_event.is_set():
            return False

        reservation = self.reserve_task()

        if reservation is None:
            return False

        task_id, lease_token = reservation

        logger.info(
            "Worker %s reserved task %s",
            self.worker_id,
            task_id,
        )

        task = self.storage.get_task(task_id)

        if task is None:

            logger.error(
                "Task %s not found in storage",
                task_id,
            )

            self.acknowledge_task(task_id, lease_token)
            return True

        heartbeat_thread = None
        stop_event = None
        lease_lost_event = None
        execution_started_at = None

        try:

            # Verify ownership before execution.
            if not self.owns_task(task_id, lease_token):

                logger.warning(
                    "Worker %s lost ownership of task %s "
                    "before execution",
                    self.worker_id,
                    task_id,
                )

                return True

            # Atomic PENDING -> RUNNING transition.
            running_task = self.transition_task(
                task_id,
                TaskState.RUNNING,
                lease_token=lease_token,
            )

            logger.info(
                "Worker %s executing %s (attempt %s/%s)",
                self.worker_id,
                task.task_type,
                running_task.attempts,
                running_task.max_attempts,
            )

            # Start heartbeat.
            (
                heartbeat_thread,
                stop_event,
                lease_lost_event,
            ) = self.start_heartbeat(
                task_id,
                lease_token,
            )

            # Measure handler execution using a monotonic clock.
            execution_started_at = time.monotonic()

            # Execute actual task handler.
            result = execute_handler(
                task.task_type,
                task.payload,
            )

            # Stop heartbeat after execution.
            self.stop_heartbeat(
                heartbeat_thread,
                stop_event,
            )

            # Check whether lease was lost.
            if (
                lease_lost_event is not None
                and lease_lost_event.is_set()
            ):

                logger.warning(
                    "Worker %s finished task %s after losing "
                    "its lease. Ignoring stale result.",
                    self.worker_id,
                    task_id,
                )

                return True

            # Verify current ownership again.
            if not self.owns_task(task_id, lease_token):

                logger.warning(
                    "Worker %s no longer owns task %s. "
                    "Ignoring stale result.",
                    self.worker_id,
                    task_id,
                )

                return True

            # Atomic RUNNING -> SUCCEEDED transition.
            self.transition_task(
                task_id,
                TaskState.SUCCEEDED,
                lease_token=lease_token,
            )

            self.record_execution_metrics(
                succeeded=True,
                duration=time.monotonic() - execution_started_at,
            )

            logger.info(
                "Task %s completed successfully",
                task_id,
            )

            logger.info("Result: %s", result)

        except Exception as error:

            logger.exception(
                "Task %s failed: %s",
                task_id,
                error,
            )

            # Stop heartbeat before recording failure.
            self.stop_heartbeat(
                heartbeat_thread,
                stop_event,
            )

            try:

                # Do not record a failure after losing ownership.
                if (
                    lease_lost_event is not None
                    and lease_lost_event.is_set()
                ):

                    logger.warning(
                        "Worker %s lost ownership of task %s. "
                        "Ignoring stale failure.",
                        self.worker_id,
                        task_id,
                    )

                    return True

                if not self.owns_task(task_id, lease_token):

                    logger.warning(
                        "Worker %s no longer owns task %s. "
                        "Ignoring stale failure.",
                        self.worker_id,
                        task_id,
                    )

                    return True

                # Atomic RUNNING -> FAILED transition.
                failed_task = self.transition_task(
                    task_id,
                    TaskState.FAILED,
                    lease_token=lease_token,
                    error=str(error),
                )

                if execution_started_at is not None:
                    self.record_execution_metrics(
                        succeeded=False,
                        duration=time.monotonic() - execution_started_at,
                    )

                # Atomic FAILED -> DELAYED or DEAD transition.
                self.schedule_retry(
                    task_id,
                    failed_task,
                    lease_token=lease_token,
                )

            except Exception:

                logger.exception(
                    "Could not record failure/retry "
                    "for task %s",
                    task_id,
                )

        finally:

            self.stop_heartbeat(
                heartbeat_thread,
                stop_event,
            )

            self.acknowledge_task(
                task_id,
                lease_token,
            )

        return True

    # --------------------------------------------------
    # 10. Worker loop
    # --------------------------------------------------

    def run_forever(self, poll_interval=1):

        self.shutdown_event.clear()
        self.running = True
        self.update_metrics_status("running")
        self.start_worker_heartbeat()

        logger.info(
            "Worker %s started",
            self.worker_id,
        )

        # Handle operating-system shutdown signals gracefully.
        # Signal handlers can only be installed from the main thread.
        previous_handlers = {}
        signals_to_handle = [signal.SIGINT]

        if hasattr(signal, "SIGTERM"):
            signals_to_handle.append(signal.SIGTERM)

        def handle_shutdown_signal(signum, frame):
            logger.info(
                "Worker %s received shutdown signal %s",
                self.worker_id,
                signum,
            )
            self.stop()

        if threading.current_thread() is threading.main_thread():
            for signum in signals_to_handle:
                try:
                    previous_handlers[signum] = signal.getsignal(signum)
                    signal.signal(signum, handle_shutdown_signal)
                except (ValueError, OSError):
                    logger.warning(
                        "Could not install handler for signal %s",
                        signum,
                    )

        try:
            while self.running and not self.shutdown_event.is_set():

                task_found = self.process_one()

                if not task_found:
                    # Wait can be interrupted immediately by stop().
                    self.shutdown_event.wait(poll_interval)

        finally:
            self.running = False
            self.shutdown_event.set()
            self.stop_worker_heartbeat()
            self.update_metrics_status("stopped")

            # Restore the process's original signal handlers.
            for signum, previous_handler in previous_handlers.items():
                try:
                    signal.signal(signum, previous_handler)
                except (ValueError, OSError):
                    logger.warning(
                        "Could not restore handler for signal %s",
                        signum,
                    )

            logger.info(
                "Worker %s stopped",
                self.worker_id,
            )

    # --------------------------------------------------
    # 11. Stop worker
    # --------------------------------------------------

    def stop(self):

        # Signal the worker loop to stop accepting new tasks.
        self.shutdown_event.set()
        self.running = False

        logger.info(
            "Stopping worker %s",
            self.worker_id,
        )
