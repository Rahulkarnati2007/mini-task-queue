
import argparse
import logging

from app.storage.redis_client import get_redis_client
from app.storage.processing_queue import ProcessingQueue
from app.workers.recovery_worker import RecoveryWorker


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def start_recovery_worker(
    visibility_timeout,
    poll_interval,
    batch_size
):

    redis_client = get_redis_client()

    try:

        redis_client.ping()

        logger.info("Recovery process connected to Redis")

        processing_queue = ProcessingQueue(
            redis_client=redis_client
        )

        recovery_worker = RecoveryWorker(
            processing_queue=processing_queue,
            visibility_timeout=visibility_timeout,
            poll_interval=poll_interval,
            batch_size=batch_size
        )

        recovery_worker.run_forever()

    except KeyboardInterrupt:

        logger.info("Recovery process interrupted")

    except Exception:

        logger.exception("Recovery process encountered an error")
        raise

    finally:

        redis_client.close()

        logger.info("Recovery process disconnected from Redis")


def main():

    parser = argparse.ArgumentParser(
        description="Start the automatic task recovery process"
    )

    parser.add_argument(
        "--visibility-timeout",
        type=int,
        default=30
    )

    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100
    )

    args = parser.parse_args()

    if args.visibility_timeout <= 0:
        parser.error("Visibility timeout must be positive")

    if args.poll_interval <= 0:
        parser.error("Polling interval must be positive")

    if args.batch_size <= 0:
        parser.error("Batch size must be positive")

    start_recovery_worker(
        visibility_timeout=args.visibility_timeout,
        poll_interval=args.poll_interval,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
