from unittest.mock import Mock

from app.storage.dead_letter_queue import DeadLetterQueue


def test_add_task_to_dlq():

    redis = Mock()
    queue = DeadLetterQueue(redis)

    result = queue.add_task("task-1")

    assert result is True
    redis.zadd.assert_called_once()


def test_get_all_tasks():

    redis = Mock()
    redis.zrange.return_value = ["task-1", "task-2"]

    queue = DeadLetterQueue(redis)

    result = queue.get_all_tasks()

    assert result == ["task-1", "task-2"]


def test_remove_task():

    redis = Mock()
    redis.zrem.return_value = 1

    queue = DeadLetterQueue(redis)

    assert queue.remove_task("task-1") is True


def test_remove_missing_task():

    redis = Mock()
    redis.zrem.return_value = 0

    queue = DeadLetterQueue(redis)

    assert queue.remove_task("task-1") is False


def test_contains_task():

    redis = Mock()
    redis.zscore.return_value = 12345

    queue = DeadLetterQueue(redis)

    assert queue.contains("task-1") is True


def test_task_not_in_dlq():

    redis = Mock()
    redis.zscore.return_value = None

    queue = DeadLetterQueue(redis)

    assert queue.contains("task-1") is False


def test_get_queue_size():

    redis = Mock()
    redis.zcard.return_value = 5

    queue = DeadLetterQueue(redis)

    assert queue.get_queue_size() == 5