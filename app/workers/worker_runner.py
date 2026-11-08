import argparse
import logging

from app.storage.redis_client import get_redis_client
from app.storage.ready_queue import ReadyQueue
from app.storage.processing_queue import ProcessingQueue
from app.storage.delayed_queue import DelayedQueue
from app.storage.dead_letter_queue import DeadLetterQueue
from app.storage.task_storage import TaskStorage

from app.core.state_manager import TaskStateManager
from app.core.worker_metrics import WorkerMetrics

from app.workers.worker import Worker
from app.workers.retry_policy import RetryPolicy


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def start_worker(worker_id: str, poll_interval: float):

    redis_client = get_redis_client()

    try:
        redis_client.ping()

        logger.info(
            "Worker %s connecting to Redis",
            worker_id
        )

        task_storage = TaskStorage(redis_client)
        ready_queue = ReadyQueue(redis_client)
        processing_queue = ProcessingQueue(redis_client)
        delayed_queue = DelayedQueue(redis_client)
        dead_letter_queue = DeadLetterQueue(redis_client)

        state_manager = TaskStateManager(task_storage)
        worker_metrics = WorkerMetrics(redis_client)

        retry_policy = RetryPolicy(
            base_delay=2,
            max_delay=60
        )

        worker = Worker(
            worker_id=worker_id,
            ready_queue=ready_queue,
            task_storage=task_storage,
            state_manager=state_manager,
            processing_queue=processing_queue,
            delayed_queue=delayed_queue,
            retry_policy=retry_policy,
            dead_letter_queue=dead_letter_queue,
            metrics=worker_metrics,
            worker_heartbeat_interval=5,
        )

        logger.info(
            "Worker %s is ready",
            worker_id
        )

        worker.run_forever(
            poll_interval=poll_interval
        )

    except KeyboardInterrupt:
        logger.info(
            "Worker %s interrupted",
            worker_id
        )

    except Exception:
        logger.exception(
            "Worker %s encountered an error",
            worker_id
        )
        raise

    finally:
        redis_client.close()

        logger.info(
            "Worker %s disconnected from Redis",
            worker_id
        )


def main():

    parser = argparse.ArgumentParser(
        description="Start a Redis-backed task queue worker"
    )

    parser.add_argument(
        "--worker-id",
        required=True
    )

    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0
    )

    args = parser.parse_args()

    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")

    start_worker(
        worker_id=args.worker_id,
        poll_interval=args.poll_interval
    )


if __name__ == "__main__":
    main()