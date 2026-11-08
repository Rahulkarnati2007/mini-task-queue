
import time
import uuid

import pytest
import redis

from app.storage.delayed_queue import DelayedQueue


@pytest.fixture
def redis_client():
    client = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=True
    )

    try:
        client.ping()
    except redis.RedisError:
        pytest.fail("Redis is not running on localhost:6379")

    yield client


@pytest.fixture
def atomic_test_setup(redis_client):

    suffix = uuid.uuid4().hex

    task_id = f"atomic-test-{suffix}"
    task_key = f"task:{task_id}"

    delayed_key = f"test:delayed:{suffix}"
    ready_key = f"test:ready:{suffix}"
    sequence_key = f"test:ready:sequence:{suffix}"

    delayed_queue = DelayedQueue(
        redis_client,
        queue_key=delayed_key
    )

    yield {
        "redis": redis_client,
        "task_id": task_id,
        "task_key": task_key,
        "delayed_key": delayed_key,
        "ready_key": ready_key,
        "sequence_key": sequence_key,
        "queue": delayed_queue,
    }

    redis_client.delete(
        task_key,
        delayed_key,
        ready_key,
        sequence_key
    )


def test_atomic_delayed_to_pending_success(atomic_test_setup):

    data = atomic_test_setup

    client = data["redis"]
    task_id = data["task_id"]
    task_key = data["task_key"]
    queue = data["queue"]

    now = time.time()

    client.hset(
        task_key,
        mapping={
            "state": "delayed",
            "priority": "8",
            "attempts": "1",
        }
    )

    client.zadd(
        data["delayed_key"],
        {task_id: now - 5}
    )

    result = queue.transition_to_pending(
        task_id=task_id,
        updates={
            "state": "pending",
            "updated_at": "test-time",
            "retry_at": "",
        },
        ready_queue_key=data["ready_key"],
        sequence_key=data["sequence_key"],
        now_timestamp=now,
    )

    assert result == 1

    assert client.hget(task_key, "state") == "pending"

    assert client.zscore(
        data["delayed_key"],
        task_id
    ) is None

    assert client.zscore(
        data["ready_key"],
        task_id
    ) is not None


def test_priority_is_preserved(atomic_test_setup):

    data = atomic_test_setup

    client = data["redis"]
    task_id = data["task_id"]

    client.hset(
        data["task_key"],
        mapping={
            "state": "delayed",
            "priority": "9",
        }
    )

    client.zadd(
        data["delayed_key"],
        {task_id: time.time() - 5}
    )

    result = data["queue"].transition_to_pending(
        task_id=task_id,
        updates={"state": "pending"},
        ready_queue_key=data["ready_key"],
        sequence_key=data["sequence_key"],
    )

    assert result == 1

    score = client.zscore(
        data["ready_key"],
        task_id
    )

    assert score is not None

    sequence = client.get(data["sequence_key"])

    expected_score = (
        (9 - 9) * 10**12 + int(sequence)
    )

    assert score == expected_score


def test_future_retry_time_not_promoted(atomic_test_setup):

    data = atomic_test_setup

    client = data["redis"]
    task_id = data["task_id"]

    future_time = time.time() + 300

    client.hset(
        data["task_key"],
        mapping={
            "state": "delayed",
            "priority": "5",
        }
    )

    client.zadd(
        data["delayed_key"],
        {task_id: future_time}
    )

    result = data["queue"].transition_to_pending(
        task_id=task_id,
        updates={"state": "pending"},
        ready_queue_key=data["ready_key"],
        sequence_key=data["sequence_key"],
        now_timestamp=time.time(),
    )

    assert result == -2

    assert client.hget(data["task_key"], "state") == "delayed"

    assert client.zscore(
        data["ready_key"],
        task_id
    ) is None


def test_wrong_state_cannot_be_promoted(atomic_test_setup):

    data = atomic_test_setup

    client = data["redis"]
    task_id = data["task_id"]

    client.hset(
        data["task_key"],
        mapping={
            "state": "running",
            "priority": "5",
        }
    )

    client.zadd(
        data["delayed_key"],
        {task_id: time.time() - 5}
    )

    result = data["queue"].transition_to_pending(
        task_id=task_id,
        updates={"state": "pending"},
        ready_queue_key=data["ready_key"],
        sequence_key=data["sequence_key"],
    )

    assert result == -1

    assert client.hget(data["task_key"], "state") == "running"

    assert client.zscore(
        data["ready_key"],
        task_id
    ) is None


def test_missing_task_returns_zero(atomic_test_setup):

    data = atomic_test_setup

    result = data["queue"].transition_to_pending(
        task_id=data["task_id"],
        updates={"state": "pending"},
        ready_queue_key=data["ready_key"],
        sequence_key=data["sequence_key"],
    )

    assert result == 0


def test_missing_delayed_entry_returns_error(atomic_test_setup):

    data = atomic_test_setup

    client = data["redis"]
    task_id = data["task_id"]

    client.hset(
        data["task_key"],
        mapping={
            "state": "delayed",
            "priority": "5",
        }
    )

    result = data["queue"].transition_to_pending(
        task_id=task_id,
        updates={"state": "pending"},
        ready_queue_key=data["ready_key"],
        sequence_key=data["sequence_key"],
    )

    assert result == -3

    assert client.hget(data["task_key"], "state") == "delayed"
