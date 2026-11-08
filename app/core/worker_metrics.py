import time


class WorkerMetrics:
    """
    Store per-worker execution metrics and liveness heartbeats in Redis.

    Each worker has its own Redis hash:
        workers:metrics:<worker_id>
    """

    KEY_PREFIX = "workers:metrics:"

    def __init__(self, redis_client):
        self.redis = redis_client

    def _key(self, worker_id):
        return f"{self.KEY_PREFIX}{worker_id}"

    def set_status(self, worker_id, status):
        """Set worker status and its latest update timestamp."""
        now = time.time()
        values = {
            "status": status,
            "updated_at": now,
        }

        if status == "running":
            values["started_at"] = now
        elif status == "stopped":
            values["stopped_at"] = now

        self.redis.hset(self._key(worker_id), mapping=values)

    def record_heartbeat(self, worker_id):
        """
        Record that a worker process is alive.

        This heartbeat is independent of task lease renewal and therefore
        continues while the worker is idle and has no task to process.
        """
        now = time.time()
        self.redis.hset(
            self._key(worker_id),
            mapping={
                "heartbeat_at": now,
                "updated_at": now,
            },
        )

    def record_execution(self, worker_id, succeeded, duration):
        """Atomically update counters and execution-time totals."""
        duration = max(0.0, float(duration))
        key = self._key(worker_id)

        pipe = self.redis.pipeline()
        pipe.hincrby(key, "tasks_processed", 1)

        if succeeded:
            pipe.hincrby(key, "tasks_succeeded", 1)
        else:
            pipe.hincrby(key, "tasks_failed", 1)

        pipe.hincrbyfloat(key, "total_execution_time", duration)
        pipe.hset(
            key,
            mapping={
                "last_execution_time": duration,
                "last_execution_at": time.time(),
                "updated_at": time.time(),
            },
        )
        pipe.execute()

    def get_worker_metrics(self, worker_id):
        """Return normalized metrics for one worker."""
        raw = self.redis.hgetall(self._key(worker_id))

        def as_int(name):
            return int(raw.get(name, 0))

        def as_float(name):
            return float(raw.get(name, 0.0))

        processed = as_int("tasks_processed")
        total_time = as_float("total_execution_time")

        return {
            "worker_id": worker_id,
            "status": raw.get("status", "unknown"),
            "tasks_processed": processed,
            "tasks_succeeded": as_int("tasks_succeeded"),
            "tasks_failed": as_int("tasks_failed"),
            "total_execution_time": total_time,
            "average_execution_time": (
                total_time / processed if processed else 0.0
            ),
            "last_execution_time": as_float("last_execution_time"),
            "started_at": as_float("started_at"),
            "stopped_at": as_float("stopped_at"),
            "updated_at": as_float("updated_at"),
            "heartbeat_at": as_float("heartbeat_at"),
        }

    def get_worker_health(self, worker_id, stale_after_seconds=15):
        """
        Classify a worker as healthy, unhealthy, or stopped.

        A running worker is unhealthy when it has not sent a heartbeat
        within stale_after_seconds. A stopped worker is not treated as
        unhealthy because it has shut down intentionally.
        """
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")

        raw = self.redis.hgetall(self._key(worker_id))
        worker_status = raw.get("status", "unknown")
        heartbeat_at = float(raw.get("heartbeat_at", 0.0) or 0.0)

        if worker_status == "stopped":
            health = "stopped"
            heartbeat_age = (
                max(0.0, time.time() - heartbeat_at)
                if heartbeat_at else None
            )
        elif worker_status != "running":
            health = "unknown"
            heartbeat_age = (
                max(0.0, time.time() - heartbeat_at)
                if heartbeat_at else None
            )
        elif not heartbeat_at:
            health = "unhealthy"
            heartbeat_age = None
        else:
            heartbeat_age = max(0.0, time.time() - heartbeat_at)
            health = (
                "healthy"
                if heartbeat_age <= stale_after_seconds
                else "unhealthy"
            )

        return {
            "worker_id": worker_id,
            "status": worker_status,
            "health": health,
            "last_heartbeat_at": heartbeat_at or None,
            "heartbeat_age_seconds": heartbeat_age,
            "stale_after_seconds": float(stale_after_seconds),
        }

    def get_all_worker_health(self, stale_after_seconds=15):
        """Return health information for every worker known to Redis."""
        workers = []

        for key in self.redis.scan_iter(
            match=f"{self.KEY_PREFIX}*",
            count=500,
        ):
            worker_id = key[len(self.KEY_PREFIX):]
            workers.append(
                self.get_worker_health(
                    worker_id,
                    stale_after_seconds=stale_after_seconds,
                )
            )

        return sorted(workers, key=lambda item: item["worker_id"])

    def get_all_worker_metrics(self):
        """Return metrics for all workers that have reported to Redis."""
        workers = []

        for key in self.redis.scan_iter(
            match=f"{self.KEY_PREFIX}*",
            count=500,
        ):
            worker_id = key[len(self.KEY_PREFIX):]
            workers.append(self.get_worker_metrics(worker_id))

        return sorted(workers, key=lambda item: item["worker_id"])
