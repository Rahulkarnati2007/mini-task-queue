from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
from types import SimpleNamespace
import time
import uuid
from unittest.mock import Mock

import pytest

from app.storage.redis_client import get_redis_client
from app.storage.processing_queue import ProcessingQueue
from app.workers.worker import Worker
from app.core.task import TaskState


@pytest.fixture
def worker_concurrency_setup(monkeypatch):
    client = get_redis_client()

    try:
        client.ping()
    except Exception:
        pytest.skip("Redis is not available")

    suffix = uuid.uuid4().hex
    ready_key = f"test:worker-concurrency:{suffix}:ready"
    processing_key = f"test:worker-concurrency:{suffix}:processing"

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


def test_multiple_worker_instances_execute_each_task_once(
    worker_concurrency_setup,
    monkeypatch,
):
    client, processing_queue = worker_concurrency_setup

    worker_count = 4
    task_count = 60

    tasks = {
        f"worker-execution-task-{index}": SimpleNamespace(
            id=f"worker-execution-task-{index}",
            task_type="test_handler",
            payload={"task_number": index},
            attempts=0,
            max_attempts=3,
        )
        for index in range(task_count)
    }

    # The real Redis sorted set is shared by all Worker instances.
    client.zadd(
        processing_queue.ready_queue_key,
        {task_id: index for index, task_id in enumerate(tasks)},
    )

    class InMemoryTaskStorage:
        def get_task(self, task_id):
            return tasks.get(task_id)

    task_storage = InMemoryTaskStorage()
    state_manager = Mock()

    # The state manager returns the task object for both transitions.
    state_manager.transition_task.side_effect = (
        lambda task_id, new_state, **kwargs: tasks[task_id]
    )

    execution_counts = {}
    execution_lock = Lock()

    def fake_handler(task_type, payload):
        task_number = payload["task_number"]

        with execution_lock:
            execution_counts[task_number] = (
                execution_counts.get(task_number, 0) + 1
            )

        # Simulate work so the worker threads overlap in execution.
        time.sleep(0.01)
        return {"processed": task_number}

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        fake_handler,
    )

    workers = [
        Worker(
            worker_id=f"integration-worker-{index}",
            ready_queue=Mock(),
            task_storage=task_storage,
            state_manager=state_manager,
            processing_queue=processing_queue,
            heartbeat_interval=10,
        )
        for index in range(worker_count)
    ]

    start_barrier = Barrier(worker_count)

    def run_worker(worker):
        start_barrier.wait()
        processed = 0

        while worker.process_one():
            processed += 1

        return processed

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(run_worker, worker)
            for worker in workers
        ]
        worker_results = [
            future.result(timeout=30)
            for future in futures
        ]

    # All tasks were handled, with no duplicate handler executions.
    assert sum(worker_results) == task_count
    assert len(execution_counts) == task_count
    assert all(count == 1 for count in execution_counts.values())
    assert set(execution_counts) == set(range(task_count))

    # Every task completed its RUNNING -> SUCCEEDED transitions.
    running_calls = [
        call for call in state_manager.transition_task.call_args_list
        if call.args[1] == TaskState.RUNNING
    ]
    succeeded_calls = [
        call for call in state_manager.transition_task.call_args_list
        if call.args[1] == TaskState.SUCCEEDED
    ]

    assert len(running_calls) == task_count
    assert len(succeeded_calls) == task_count

    # No task remains reserved or waiting in the ready queue.
    assert processing_queue.get_queue_size() == 0
    assert client.zcard(processing_queue.ready_queue_key) == 0
    assert client.hlen(processing_queue.lease_key) == 0
    assert client.hlen(processing_queue.owner_key) == 0
