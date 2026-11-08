from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

from app.storage.delayed_queue import DelayedQueue


def test_enqueue_task():
    redis = Mock()
    queue = DelayedQueue(redis)

    retry_at = datetime.now(timezone.utc) + timedelta(seconds=5)

    queue.enqueue("task-1", retry_at)

    redis.zadd.assert_called_once_with(
        "tasks:delayed",
        {"task-1": retry_at.timestamp()}
    )


def test_get_due_tasks():
    redis = Mock()
    redis.zrangebyscore.return_value = ["task-1"]

    queue = DelayedQueue(redis)

    result = queue.get_due_tasks(limit=10)

    assert result == ["task-1"]
    redis.zrangebyscore.assert_called_once()


def test_remove_task():
    redis = Mock()
    redis.zrem.return_value = 1

    queue = DelayedQueue(redis)

    assert queue.remove_task("task-1") is True


def test_remove_missing_task():
    redis = Mock()
    redis.zrem.return_value = 0

    queue = DelayedQueue(redis)

    assert queue.remove_task("task-1") is False


def test_queue_size():
    redis = Mock()
    redis.zcard.return_value = 3

    queue = DelayedQueue(redis)

    assert queue.get_queue_size() == 3


def test_get_retry_at():
    redis = Mock()

    timestamp = datetime.now(timezone.utc).timestamp()
    redis.zscore.return_value = timestamp

    queue = DelayedQueue(redis)

    result = queue.get_retry_at("task-1")

    assert result is not None
    assert abs(result.timestamp() - timestamp) < 0.001