from unittest.mock import Mock

from app.core.queue_stats import QueueStatsService


def create_redis_mock():

    redis = Mock()

    redis.zcard.side_effect = [
        5,  # ready
        2,  # processing
        3,  # delayed
        1   # dead
    ]

    return redis


def test_queue_statistics():

    redis = create_redis_mock()

    pipeline = redis.pipeline.return_value

    redis.scan_iter.return_value = iter([
        "task:1",
        "task:2",
        "task:3"
    ])

    pipeline.execute.return_value = [
        "pending",
        "running",
        "dead"
    ]

    service = QueueStatsService(redis)

    result = service.get_queue_stats()

    assert result["queues"] == {
        "ready": 5,
        "processing": 2,
        "delayed": 3,
        "dead": 1
    }

    assert result["total_tasks"] == 3

    assert result["states"]["pending"] == 1
    assert result["states"]["running"] == 1
    assert result["states"]["dead"] == 1

    assert result["active_tasks"] == 2


def test_empty_task_database():

    redis = create_redis_mock()

    redis.zcard.side_effect = [0, 0, 0, 0]
    redis.scan_iter.return_value = iter([])

    service = QueueStatsService(redis)

    result = service.get_queue_stats()

    assert result["total_tasks"] == 0
    assert result["active_tasks"] == 0

    assert all(
        value == 0
        for value in result["states"].values()
    )


def test_all_task_states_are_initialized():

    redis = create_redis_mock()

    redis.scan_iter.return_value = iter([])
    redis.pipeline.return_value.execute.return_value = []

    service = QueueStatsService(redis)

    result = service.get_queue_stats()

    expected_states = [
        "pending",
        "running",
        "succeeded",
        "failed",
        "delayed",
        "dead",
        "cancelled"
    ]

    assert list(result["states"].keys()) == expected_states


def test_unknown_state_is_not_counted_as_known_state():

    redis = create_redis_mock()

    redis.scan_iter.return_value = iter([
        "task:1",
        "task:2"
    ])

    redis.pipeline.return_value.execute.return_value = [
        "pending",
        "unknown_state"
    ]

    service = QueueStatsService(redis)

    result = service.get_queue_stats()

    assert result["total_tasks"] == 2
    assert result["states"]["pending"] == 1
    assert result["states"]["dead"] == 0