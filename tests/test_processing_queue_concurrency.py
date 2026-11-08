from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import uuid

import pytest

from app.storage.redis_client import get_redis_client
from app.storage.processing_queue import ProcessingQueue


@pytest.fixture
def concurrency_queue():
    client = get_redis_client()

    try:
        client.ping()
    except Exception:
        pytest.skip("Redis is not available")

    suffix = uuid.uuid4().hex
    ready_key = f"test:concurrency:{suffix}:ready"
    processing_key = f"test:concurrency:{suffix}:processing"

    queue = ProcessingQueue(
        redis_client=client,
        ready_queue_key=ready_key,
        processing_queue_key=processing_key,
    )

    yield queue

    client.delete(
        ready_key,
        processing_key,
        f"{processing_key}:leases",
        f"{processing_key}:owners",
        f"{ready_key}:sequence",
    )
    client.close()


def test_multiple_workers_reserve_each_task_only_once(concurrency_queue):
    client = concurrency_queue.redis

    worker_count = 8
    task_count = 100

    tasks = {
        f"concurrent-task-{index}": index
        for index in range(task_count)
    }
    client.zadd(concurrency_queue.ready_queue_key, tasks)

    start_barrier = Barrier(worker_count)

    def reserve_as_worker(worker_number):
        worker_id = f"concurrent-worker-{worker_number}"
        reservations = []

        # Make all workers begin competing at approximately the same time.
        start_barrier.wait()

        while True:
            reservation = concurrency_queue.reserve_task_with_lease(
                worker_id
            )

            if reservation is None:
                break

            task_id, lease_token = reservation
            reservations.append((task_id, lease_token, worker_id))

        return reservations

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(reserve_as_worker, worker_number)
            for worker_number in range(worker_count)
        ]

        all_reservations = []
        for future in futures:
            all_reservations.extend(future.result(timeout=15))

    reserved_task_ids = [
        task_id for task_id, _, _ in all_reservations
    ]
    lease_tokens = [
        lease_token for _, lease_token, _ in all_reservations
    ]

    # Every queued task must be reserved.
    assert len(all_reservations) == task_count

    # No task may be claimed by more than one worker.
    assert len(reserved_task_ids) == len(set(reserved_task_ids))
    assert set(reserved_task_ids) == set(tasks)

    # Each reservation must have its own non-empty lease token.
    assert all(lease_tokens)
    assert len(lease_tokens) == len(set(lease_tokens))

    # Every task must have the matching worker owner and lease in Redis.
    for task_id, lease_token, worker_id in all_reservations:
        assert concurrency_queue.get_task_lease(task_id) == lease_token
        assert concurrency_queue.get_task_owner(task_id) == worker_id
        assert concurrency_queue.contains(task_id)

    # All tasks have left the ready queue and are in processing.
    assert client.zcard(concurrency_queue.ready_queue_key) == 0
    assert concurrency_queue.get_queue_size() == task_count


def test_only_current_worker_can_acknowledge_concurrently_reserved_task(
    concurrency_queue,
):
    client = concurrency_queue.redis
    task_id = "concurrent-ack-task"

    client.zadd(concurrency_queue.ready_queue_key, {task_id: 1})

    reservation = concurrency_queue.reserve_task_with_lease("worker-current")
    assert reservation is not None

    reserved_task_id, current_token = reservation
    assert reserved_task_id == task_id

    # A stale worker must not acknowledge the current worker's reservation.
    assert concurrency_queue.acknowledge_task(
        task_id,
        "stale-worker-token",
    ) is False

    assert concurrency_queue.contains(task_id)
    assert concurrency_queue.get_task_lease(task_id) == current_token
    assert concurrency_queue.get_task_owner(task_id) == "worker-current"

    # The current worker can acknowledge it.
    assert concurrency_queue.acknowledge_task(
        task_id,
        current_token,
    ) is True

    assert concurrency_queue.contains(task_id) is False
    assert concurrency_queue.get_task_lease(task_id) is None
    assert concurrency_queue.get_task_owner(task_id) is None
    assert client.zcard(concurrency_queue.ready_queue_key) == 0
