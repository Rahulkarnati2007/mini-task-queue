
import time
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


class RecoveryWorker:
    """
    Background process that periodically checks
    for expired tasks and recovers them.
    """

    def __init__(
        self,
        processing_queue,
        visibility_timeout=30,
        poll_interval=5,
        batch_size=100
    ):
        self.processing_queue = processing_queue

        self.visibility_timeout = visibility_timeout
        self.poll_interval = poll_interval
        self.batch_size = batch_size

        self.running = False

    # ---------------------------------------
    # 1. Run one recovery check
    # ---------------------------------------

    def recover_once(self):

        recovered_tasks = (
            self.processing_queue.recover_expired_tasks(
                visibility_timeout=self.visibility_timeout,
                batch_size=self.batch_size
            )
        )

        if recovered_tasks:

            logger.warning(
                "Recovered %d expired tasks: %s",
                len(recovered_tasks),
                recovered_tasks
            )

        else:

            logger.info("No expired tasks found")

        return recovered_tasks

    # ---------------------------------------
    # 2. Run recovery continuously
    # ---------------------------------------

    def run_forever(self):

        self.running = True

        logger.info(
            "Recovery worker started | Timeout: %s seconds | "
            "Polling interval: %s seconds",
            self.visibility_timeout,
            self.poll_interval
        )

        try:

            while self.running:

                try:

                    self.recover_once()

                except Exception:

                    logger.exception(
                        "Recovery check failed"
                    )

                if self.running:
                    time.sleep(self.poll_interval)

        except KeyboardInterrupt:

            logger.info("Recovery worker interrupted")

        finally:

            self.running = False

            logger.info("Recovery worker stopped")

    # ---------------------------------------
    # 3. Stop recovery worker
    # ---------------------------------------

    def stop(self):

        self.running = False

        logger.info("Stopping recovery worker")
