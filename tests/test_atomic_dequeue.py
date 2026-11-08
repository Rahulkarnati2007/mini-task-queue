
import uuid

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.storage.redis_client import get_redis_client
from app.storage.ready_queue import ReadyQueue


# ---------------------------------------
# Redis test fixture
# ---------------------------------------

@pytest.fixture
def ready_queue():

    redis_client = get_redis_client()

    assert redis_client.ping()

    queue_key = f"test:atomic:{uuid.uuid4()}"

    queue = ReadyQueue(
        redis_client,
        queue_key=queue_key
    )

    yield queue

    # Clean up test data
    redis_client.delete(
        queue_key,
        queue.sequence_key
    )


# ---------------------------------------
# 1. Dequeue highest-priority task
# ---------------------------------------

def test_dequeue_highest_priority_first(ready_queue):

    ready_queue.enqueue("task_low", 2)

    ready_queue.enqueue("task_high", 9)

    ready_queue.enqueue("task_medium", 5)

    task_id = ready_queue.dequeue()

    assert task_id == "task_high"


# ---------------------------------------
# 2. Verify dequeued task is removed
# ---------------------------------------

def test_dequeue_removes_task_from_queue(ready_queue):

    ready_queue.enqueue("task_101", 8)

    assert ready_queue.contains("task_101")

    task_id = ready_queue.dequeue()

    assert task_id == "task_101"

    assert not ready_queue.contains("task_101")


# ---------------------------------------
# 3. Verify FIFO for equal priorities
# ---------------------------------------

def test_dequeue_preserves_fifo_order(ready_queue):

    ready_queue.enqueue("task_A", 5)

    ready_queue.enqueue("task_B", 5)

    ready_queue.enqueue("task_C", 5)

    assert ready_queue.dequeue() == "task_A"

    assert ready_queue.dequeue() == "task_B"

    assert ready_queue.dequeue() == "task_C"


# ---------------------------------------
# 4. Dequeue from empty queue
# ---------------------------------------

def test_dequeue_empty_queue(ready_queue):

    task_id = ready_queue.dequeue()

    assert task_id is None


# ---------------------------------------
# 5. Concurrent workers must not
#    receive duplicate tasks
# ---------------------------------------

def test_concurrent_dequeue_no_duplicates(ready_queue):

    total_tasks = 100

    number_of_workers = 8

    # Insert 100 tasks
    for i in range(total_tasks):

        ready_queue.enqueue(
            task_id=f"task_{i}",
            priority=5
        )

    # Each simulated worker continuously
    # dequeues until the queue is empty.
    def worker():

        received_tasks = []

        while True:

            task_id = ready_queue.dequeue()

            if task_id is None:
                break

            received_tasks.append(task_id)

        return received_tasks

    # Run multiple workers concurrently
    with ThreadPoolExecutor(
        max_workers=number_of_workers
    ) as executor:

        results = list(
            executor.map(
                lambda _: worker(),
                range(number_of_workers)
            )
        )

    # Combine tasks received by all workers
    all_received_tasks = []

    for worker_tasks in results:

        all_received_tasks.extend(worker_tasks)

    # Verify all tasks were received
    assert len(all_received_tasks) == total_tasks

    # Verify no duplicate task was received
    assert len(set(all_received_tasks)) == total_tasks

    # Verify all expected tasks were received
    assert set(all_received_tasks) == {
        f"task_{i}"
        for i in range(total_tasks)
    }

    # Queue must be empty
    assert ready_queue.get_queue_size() == 0
