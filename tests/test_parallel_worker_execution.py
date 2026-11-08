from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
from types import SimpleNamespace
import time
import uuid
from unittest.mock import Mock

import pytest

from app.core.task import TaskState
from app.storage.processing_queue import ProcessingQueue
from app.storage.redis_client import get_redis_client
from app.workers.worker import Worker


@pytest.fixture
def parallel_execution_setup():
    client = get_redis_client()

    try:
        client.ping()
    except Exception:
        pytest.skip("Redis is not available")

    suffix = uuid.uuid4().hex
    ready_key = f"test:parallel-execution:{suffix}:ready"
    processing_key = f"test:parallel-execution:{suffix}:processing"

    queue = ProcessingQueue(
        redis_client=client,
        ready_queue_key=ready_key,
        processing_queue_key=processing_key,
    )

    yield client, queue

    client.delete(
        ready_key,
        processing_key,
        f"{processing_key}:leases",
        f"{processing_key}:owners",
        f"{ready_key}:sequence",
    )
    client.close()


def test_multiple_workers_execute_handlers_at_the_same_time(
    parallel_execution_setup,
    monkeypatch,
):
    client, processing_queue = parallel_execution_setup

    worker_count = 4
    task_count = 24

    tasks = {
        f"parallel-task-{index}": SimpleNamespace(
            id=f"parallel-task-{index}",
            task_type="parallel_test",
            payload={"number": index},
            attempts=0,
            max_attempts=3,
        )
        for index in range(task_count)
    }

    client.zadd(
        processing_queue.ready_queue_key,
        {
            task_id: index
            for index, task_id in enumerate(tasks)
        },
    )

    class InMemoryTaskStorage:
        def get_task(self, task_id):
            return tasks.get(task_id)

    state_manager = Mock()
    state_manager.transition_task.side_effect = (
        lambda task_id, new_state, **kwargs: tasks[task_id]
    )

    # The first two handlers must overlap. If execution is serial,
    # the barrier times out and the test fails.
    overlap_barrier = Barrier(2)
    active_lock = Lock()
    active_handlers = 0
    peak_active_handlers = 0
    executed_tasks = []
    execution_lock = Lock()

    def parallel_handler(task_type, payload):
        nonlocal active_handlers, peak_active_handlers

        with active_lock:
            active_handlers += 1
            peak_active_handlers = max(
                peak_active_handlers,
                active_handlers,
            )

        try:
            # Require at least two handlers to be active together.
            overlap_barrier.wait(timeout=5)
            time.sleep(0.02)

            with execution_lock:
                executed_tasks.append(payload["number"])

            return {"processed": payload["number"]}
        finally:
            with active_lock:
                active_handlers -= 1

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        parallel_handler,
    )

    workers = [
        Worker(
            worker_id=f"parallel-worker-{index}",
            ready_queue=Mock(),
            task_storage=InMemoryTaskStorage(),
            state_manager=state_manager,
            processing_queue=processing_queue,
            heartbeat_interval=10,
        )
        for index in range(worker_count)
    ]

    start_barrier = Barrier(worker_count)

    def run_worker(worker):
        start_barrier.wait()
        count = 0

        while worker.process_one():
            count += 1

        return count

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(run_worker, worker)
            for worker in workers
        ]
        processed_counts = [
            future.result(timeout=30)
            for future in futures
        ]

    assert sum(processed_counts) == task_count
    assert len(executed_tasks) == task_count
    assert sorted(executed_tasks) == list(range(task_count))

    # This is the key assertion: more than one handler was active
    # at the same time.
    assert peak_active_handlers >= 2

    assert processing_queue.get_queue_size() == 0
    assert client.zcard(processing_queue.ready_queue_key) == 0
    assert client.hlen(processing_queue.lease_key) == 0
    assert client.hlen(processing_queue.owner_key) == 0

    running_count = sum(
        call.args[1] == TaskState.RUNNING
        for call in state_manager.transition_task.call_args_list
    )
    succeeded_count = sum(
        call.args[1] == TaskState.SUCCEEDED
        for call in state_manager.transition_task.call_args_list
    )

    assert running_count == task_count
    assert succeeded_count == task_count
