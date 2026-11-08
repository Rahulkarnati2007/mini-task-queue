from unittest.mock import Mock

import pytest

from app.core.worker_metrics import WorkerMetrics
from app.core.task import TaskState
from app.workers.worker import Worker


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.operations = []

    def hincrby(self, key, field, amount):
        self.operations.append(("hincrby", key, field, amount))
        return self

    def hincrbyfloat(self, key, field, amount):
        self.operations.append(("hincrbyfloat", key, field, amount))
        return self

    def hset(self, key, mapping):
        self.operations.append(("hset", key, mapping))
        return self

    def execute(self):
        for operation in self.operations:
            kind = operation[0]
            if kind == "hincrby":
                _, key, field, amount = operation
                self.redis.data.setdefault(key, {})
                self.redis.data[key][field] = (
                    int(self.redis.data[key].get(field, 0)) + amount
                )
            elif kind == "hincrbyfloat":
                _, key, field, amount = operation
                self.redis.data.setdefault(key, {})
                self.redis.data[key][field] = (
                    float(self.redis.data[key].get(field, 0.0)) + amount
                )
            elif kind == "hset":
                _, key, mapping = operation
                self.redis.data.setdefault(key, {}).update(mapping)


class FakeRedis:
    def __init__(self):
        self.data = {}

    def hset(self, key, mapping):
        self.data.setdefault(key, {}).update(mapping)

    def hgetall(self, key):
        return self.data.get(key, {}).copy()

    def pipeline(self):
        return FakePipeline(self)

    def scan_iter(self, match, count):
        prefix = match[:-1]
        return [key for key in self.data if key.startswith(prefix)]


def test_worker_metrics_records_success_and_failure():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)

    metrics.record_execution("worker-1", succeeded=True, duration=2.5)
    metrics.record_execution("worker-1", succeeded=False, duration=1.5)

    result = metrics.get_worker_metrics("worker-1")

    assert result["tasks_processed"] == 2
    assert result["tasks_succeeded"] == 1
    assert result["tasks_failed"] == 1
    assert result["total_execution_time"] == pytest.approx(4.0)
    assert result["average_execution_time"] == pytest.approx(2.0)


def test_worker_metrics_tracks_status():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)

    metrics.set_status("worker-1", "running")
    metrics.set_status("worker-1", "stopped")

    result = metrics.get_worker_metrics("worker-1")

    assert result["status"] == "stopped"
    assert result["started_at"] > 0
    assert result["stopped_at"] > 0


def test_worker_metrics_returns_zero_values_for_new_worker():
    metrics = WorkerMetrics(FakeRedis())

    result = metrics.get_worker_metrics("worker-new")

    assert result["status"] == "unknown"
    assert result["tasks_processed"] == 0
    assert result["average_execution_time"] == 0.0


def test_worker_metrics_lists_all_workers():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)

    metrics.set_status("worker-2", "running")
    metrics.set_status("worker-1", "stopped")

    results = metrics.get_all_worker_metrics()

    assert [item["worker_id"] for item in results] == [
        "worker-1",
        "worker-2",
    ]


def test_worker_records_success_metrics(monkeypatch):
    task = type("Task", (), {
        "id": "task-1",
        "task_type": "send_email",
        "payload": {},
    })()

    queue = Mock()
    queue.dequeue.return_value = task.id
    storage = Mock()
    storage.get_task.return_value = task
    state_manager = Mock()
    metrics = Mock()

    worker = Worker(
        worker_id="worker-1",
        ready_queue=queue,
        task_storage=storage,
        state_manager=state_manager,
        metrics=metrics,
    )

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        lambda task_type, payload: {"ok": True},
    )

    assert worker.process_one() is True

    metrics.record_execution.assert_called_once()
    args = metrics.record_execution.call_args
    assert args.args[0] == "worker-1"
    assert args.kwargs["succeeded"] is True
    assert args.kwargs["duration"] >= 0


def test_worker_records_failure_metrics(monkeypatch):
    task = type("Task", (), {
        "id": "task-2",
        "task_type": "send_email",
        "payload": {},
    })()

    queue = Mock()
    queue.dequeue.return_value = task.id
    storage = Mock()
    storage.get_task.return_value = task
    state_manager = Mock()

    # The worker reads attempts/max_attempts from the task returned
    # by the PENDING -> RUNNING transition for its execution log.
    running_task = type("RunningTask", (), {
        "attempts": 1,
        "max_attempts": 3,
    })()

    # FAILED task with attempts below max_attempts will be retried.
    failed_task = type("FailedTask", (), {
        "attempts": 1,
        "max_attempts": 3,
    })()

    state_manager.transition_task.side_effect = [
        running_task,  # RUNNING
        failed_task,   # FAILED
        None,          # DELAYED
    ]
    delayed_queue = Mock()
    retry_policy = Mock()
    retry_policy.calculate_delay.return_value = 1
    metrics = Mock()

    worker = Worker(
        worker_id="worker-1",
        ready_queue=queue,
        task_storage=storage,
        state_manager=state_manager,
        delayed_queue=delayed_queue,
        retry_policy=retry_policy,
        metrics=metrics,
    )

    def fail_handler(task_type, payload):
        raise ValueError("handler failed")

    monkeypatch.setattr(
        "app.workers.worker.execute_handler",
        fail_handler,
    )

    assert worker.process_one() is True

    metrics.record_execution.assert_called_once()
    args = metrics.record_execution.call_args
    assert args.args[0] == "worker-1"
    assert args.kwargs["succeeded"] is False
    assert args.kwargs["duration"] >= 0
