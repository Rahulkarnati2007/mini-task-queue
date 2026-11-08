import argparse
import logging

from app.storage.redis_client import get_redis_client
from app.storage.ready_queue import ReadyQueue
from app.storage.delayed_queue import DelayedQueue
from app.storage.task_storage import TaskStorage
from app.core.state_manager import TaskStateManager
from app.workers.delayed_scheduler import DelayedScheduler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def start_scheduler(
    poll_interval: float,
    batch_size: int
):

    redis_client = get_redis_client()

    try:
        redis_client.ping()

        logger.info("Connected to Redis")

        task_storage = TaskStorage(redis_client)
        ready_queue = ReadyQueue(redis_client)
        delayed_queue = DelayedQueue(redis_client)

        state_manager = TaskStateManager(task_storage)

        scheduler = DelayedScheduler(
            delayed_queue=delayed_queue,
            ready_queue=ready_queue,
            task_storage=task_storage,
            state_manager=state_manager,
            batch_size=batch_size
        )

        scheduler.run_forever(
            poll_interval=poll_interval
        )

    except Exception:
        logger.exception("Delayed scheduler failed")
        raise

    finally:
        redis_client.close()
        logger.info("Disconnected from Redis")


def main():

    parser = argparse.ArgumentParser(
        description="Start the delayed task scheduler"
    )

    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100
    )

    args = parser.parse_args()

    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")

    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")

    start_scheduler(
        poll_interval=args.poll_interval,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()