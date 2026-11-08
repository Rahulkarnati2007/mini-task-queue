
import pytest
import uuid

from app.storage.redis_client import get_redis_client
from app.storage.ready_queue import ReadyQueue


@pytest.fixture
def queue():

    redis_client = get_redis_client()

    assert redis_client.ping()

    # Unique queue for every test
    queue_key = f"test:ready:{uuid.uuid4()}"

    ready_queue = ReadyQueue(
        redis_client,
        queue_key=queue_key
    )

    yield ready_queue

    # Clean up test data
    redis_client.delete(
        queue_key,
        ready_queue.sequence_key
    )


def test_enqueue_task(queue):

    sequence = queue.enqueue(
        "task101",
        priority=5
    )

    assert sequence == 1
    assert queue.contains("task101")


def test_priority_ordering(queue):

    queue.enqueue("low", priority=2)
    queue.enqueue("medium", priority=5)
    queue.enqueue("high", priority=9)

    assert queue.get_all_tasks() == [
        "high",
        "medium",
        "low"
    ]


def test_fifo_same_priority(queue):

    queue.enqueue("task1", priority=5)
    queue.enqueue("task2", priority=5)
    queue.enqueue("task3", priority=5)

    assert queue.get_all_tasks() == [
        "task1",
        "task2",
        "task3"
    ]


def test_priority_before_fifo(queue):

    queue.enqueue("task1", priority=5)
    queue.enqueue("task2", priority=9)
    queue.enqueue("task3", priority=5)
    queue.enqueue("task4", priority=2)

    assert queue.get_all_tasks() == [
        "task2",
        "task1",
        "task3",
        "task4"
    ]


def test_peek_next_task(queue):

    queue.enqueue("task1", priority=5)
    queue.enqueue("task2", priority=9)

    next_task = queue.peek_next_task()

    assert next_task == "task2"

    # Peeking must not remove the task
    assert queue.get_queue_size() == 2


def test_remove_task(queue):

    queue.enqueue("task1", priority=5)

    result = queue.remove_task("task1")

    assert result is True
    assert queue.contains("task1") is False


def test_queue_size(queue):

    queue.enqueue("task1", priority=5)
    queue.enqueue("task2", priority=7)
    queue.enqueue("task3", priority=2)

    assert queue.get_queue_size() == 3


def test_invalid_priority(queue):

    with pytest.raises(ValueError):

        queue.enqueue(
            "task101",
            priority=15
        )


def test_empty_queue(queue):

    assert queue.peek_next_task() is None
    assert queue.get_all_tasks() == []
    assert queue.get_queue_size() == 0


def test_enqueue_sequence(queue):

    first = queue.enqueue("task1", priority=5)
    second = queue.enqueue("task2", priority=5)
    third = queue.enqueue("task3", priority=5)

    assert first == 1
    assert second == 2
    assert third == 3


def test_task_score(queue):

    queue.enqueue("high", priority=9)
    queue.enqueue("low", priority=2)

    high_score = queue.get_task_score("high")
    low_score = queue.get_task_score("low")

    assert high_score < low_score
