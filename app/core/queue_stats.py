class QueueStatsService:

    TASK_STATES = [
        "pending",
        "running",
        "succeeded",
        "failed",
        "delayed",
        "dead",
        "cancelled"
    ]

    def __init__(self, redis_client):
        self.redis = redis_client

    def get_queue_stats(self):

        # Get the sizes of the four queues.
        ready = self.redis.zcard("tasks:ready")
        processing = self.redis.zcard("tasks:processing")
        delayed = self.redis.zcard("tasks:delayed")
        dead = self.redis.zcard("tasks:dead")

        # Initialize every known state to zero.
        state_counts = {
            state: 0
            for state in self.TASK_STATES
        }

        total_tasks = 0

        # Read task hashes in batches using Redis SCAN.
        # SCAN avoids the blocking behavior of KEYS.
        pipeline = self.redis.pipeline(transaction=False)
        batch_size = 500
        batch_count = 0

        def process_batch():
            nonlocal total_tasks, batch_count

            if batch_count == 0:
                return

            states = pipeline.execute()

            for state in states:
                if state in state_counts:
                    state_counts[state] += 1

                total_tasks += 1

            batch_count = 0

        for task_key in self.redis.scan_iter(
            match="task:*",
            count=batch_size
        ):
            pipeline.hget(task_key, "state")
            batch_count += 1

            if batch_count >= batch_size:
                process_batch()

        process_batch()

        return {
            "total_tasks": total_tasks,
            "queues": {
                "ready": ready,
                "processing": processing,
                "delayed": delayed,
                "dead": dead
            },
            "states": state_counts,
            "active_tasks": (
                state_counts["pending"]
                + state_counts["running"]
                + state_counts["delayed"]
            )
        }