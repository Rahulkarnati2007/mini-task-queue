
import time
import pytest

from app.storage.redis_client import get_redis_client
from app.storage.processing_queue import ProcessingQueue


READY_KEY = "test:visibility:ready"
PROCESSING_KEY = "test:visibility:processing"
SEQUENCE_KEY = f"{READY_KEY}:sequence"


@pytest.fixture
def processing_queue():

    client = get_redis_client()

    try:
        client.ping()
    except Exception:
        pytest.skip("Redis is not available")

    client.delete(
        READY_KEY,
        PROCESSING_KEY,
        SEQUENCE_KEY
    )

    queue = ProcessingQueue(
        redis_client=client,
        ready_queue_key=READY_KEY,
        processing_queue_key=PROCESSING_KEY
    )

    yield queue

    client.delete(
        READY_KEY,
        PROCESSING_KEY,
        SEQUENCE_KEY,
        "task:visibility-1",
        "task:visibility-2",
        "task:visibility-3"
    )

    client.close()


def create_task(client, task_id, priority=5, state="running"):

    client.hset(
        f"task:{task_id}",
        mapping={
            "id": task_id,
            "priority": priority,
            "state": state
        }
    )


def test_expired_task_is_recovered(processing_queue):

    client = processing_queue.redis

    create_task(client, "visibility-1")

    # Make reservation timestamp old.
    client.zadd(
        PROCESSING_KEY,
        {"visibility-1": time.time() - 100}
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert recovered == ["visibility-1"]

    assert processing_queue.contains("visibility-1") is False

    assert client.zscore(
        READY_KEY,
        "visibility-1"
    ) is not None

    assert client.hget(
        "task:visibility-1",
        "state"
    ) == "pending"


def test_non_expired_task_is_not_recovered(processing_queue):

    client = processing_queue.redis

    create_task(client, "visibility-1")

    client.zadd(
        PROCESSING_KEY,
        {"visibility-1": time.time()}
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert recovered == []

    assert processing_queue.contains("visibility-1")


def test_multiple_expired_tasks_are_recovered(processing_queue):

    client = processing_queue.redis

    for task_id in [
        "visibility-1",
        "visibility-2",
        "visibility-3"
    ]:

        create_task(client, task_id)

        client.zadd(
            PROCESSING_KEY,
            {task_id: time.time() - 100}
        )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert len(recovered) == 3

    assert client.zcard(READY_KEY) == 3
    assert processing_queue.get_queue_size() == 0


def test_recovery_preserves_task_priority(processing_queue):

    client = processing_queue.redis

    create_task(
        client,
        "visibility-1",
        priority=9
    )

    create_task(
        client,
        "visibility-2",
        priority=2
    )

    client.zadd(
        PROCESSING_KEY,
        {
            "visibility-1": time.time() - 100,
            "visibility-2": time.time() - 100
        }
    )

    processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    ready_tasks = client.zrange(READY_KEY, 0, -1)

    assert ready_tasks == [
        "visibility-1",
        "visibility-2"
    ]


def test_recovery_respects_batch_size(processing_queue):

    client = processing_queue.redis

    for task_id in [
        "visibility-1",
        "visibility-2",
        "visibility-3"
    ]:

        create_task(client, task_id)

        client.zadd(
            PROCESSING_KEY,
            {task_id: time.time() - 100}
        )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30,
        batch_size=2
    )

    assert len(recovered) == 2

    assert client.zcard(READY_KEY) == 2
    assert processing_queue.get_queue_size() == 1

    assert client.zcard(PROCESSING_KEY) == 1


def test_missing_task_data_is_removed_from_processing(processing_queue):

    client = processing_queue.redis

    client.zadd(
        PROCESSING_KEY,
        {"missing-task": time.time() - 100}
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert recovered == []

    assert processing_queue.contains("missing-task") is False
