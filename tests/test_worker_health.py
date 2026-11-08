import time
from unittest.mock import Mock

from app.core.worker_metrics import WorkerMetrics
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
            if operation[0] == "hincrby":
                _, key, field, amount = operation
                self.redis.data.setdefault(key, {})
                self.redis.data[key][field] = str(
                    int(self.redis.data[key].get(field, 0)) + amount
                )
            elif operation[0] == "hincrbyfloat":
                _, key, field, amount = operation
                self.redis.data.setdefault(key, {})
                self.redis.data[key][field] = str(
                    float(self.redis.data[key].get(field, 0.0)) + amount
                )
            else:
                _, key, mapping = operation
                self.redis.hset(key, mapping=mapping)


class FakeRedis:
    def __init__(self):
        self.data = {}

    def hset(self, key, mapping):
        self.data.setdefault(key, {})
        self.data[key].update({k: str(v) for k, v in mapping.items()})

    def hgetall(self, key):
        return dict(self.data.get(key, {}))

    def scan_iter(self, match, count=500):
        prefix = match[:-1]
        return iter(k for k in self.data if k.startswith(prefix))

    def pipeline(self):
        return FakePipeline(self)


def test_record_heartbeat_updates_heartbeat_and_updated_timestamps():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)

    metrics.record_heartbeat("worker-1")

    record = redis.hgetall("workers:metrics:worker-1")
    assert float(record["heartbeat_at"]) > 0
    assert record["heartbeat_at"] == record["updated_at"]


def test_running_worker_with_recent_heartbeat_is_healthy():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)
    metrics.set_status("worker-1", "running")
    metrics.record_heartbeat("worker-1")

    health = metrics.get_worker_health("worker-1", stale_after_seconds=15)

    assert health["status"] == "running"
    assert health["health"] == "healthy"
    assert health["heartbeat_age_seconds"] <= 15


def test_running_worker_with_old_heartbeat_is_unhealthy():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)
    redis.hset(
        "workers:metrics:worker-1",
        mapping={
            "status": "running",
            "heartbeat_at": time.time() - 60,
        },
    )

    health = metrics.get_worker_health("worker-1", stale_after_seconds=15)

    assert health["health"] == "unhealthy"
    assert health["heartbeat_age_seconds"] >= 60


def test_running_worker_without_heartbeat_is_unhealthy():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)
    metrics.set_status("worker-1", "running")

    health = metrics.get_worker_health("worker-1")

    assert health["health"] == "unhealthy"
    assert health["last_heartbeat_at"] is None


def test_stopped_worker_is_reported_as_stopped():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)
    metrics.set_status("worker-1", "stopped")

    health = metrics.get_worker_health("worker-1")

    assert health["status"] == "stopped"
    assert health["health"] == "stopped"


def test_get_all_worker_health_returns_sorted_workers():
    redis = FakeRedis()
    metrics = WorkerMetrics(redis)

    for worker_id in ["worker-2", "worker-1"]:
        metrics.set_status(worker_id, "running")
        metrics.record_heartbeat(worker_id)

    result = metrics.get_all_worker_health()

    assert [item["worker_id"] for item in result] == [
        "worker-1",
        "worker-2",
    ]
    assert all(item["health"] == "healthy" for item in result)


def test_worker_sends_heartbeat_while_idle():
    metrics = Mock()
    worker = Worker(
        worker_id="worker-1",
        ready_queue=Mock(),
        task_storage=Mock(),
        state_manager=Mock(),
        metrics=metrics,
        worker_heartbeat_interval=0.02,
    )

    worker.start_worker_heartbeat()
    try:
        deadline = time.time() + 1
        while metrics.record_heartbeat.call_count < 2 and time.time() < deadline:
            time.sleep(0.005)

        assert metrics.record_heartbeat.call_count >= 2
    finally:
        worker.stop_worker_heartbeat()

    calls_after_stop = metrics.record_heartbeat.call_count
    time.sleep(0.04)
    assert metrics.record_heartbeat.call_count == calls_after_stop
    assert worker.worker_heartbeat_thread is None
