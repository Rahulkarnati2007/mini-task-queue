
class ReadyQueue:
    """
    Manages tasks waiting for execution
    using a Redis sorted set.
    """

    PRIORITY_BASE = 10**12
    MAX_PRIORITY = 9

    def __init__(
        self,
        redis_client,
        queue_key="tasks:ready"
    ):

        self.redis = redis_client

        self.queue_key = queue_key

        # Separate counter for enqueue sequence
        self.sequence_key = f"{queue_key}:sequence"

    # ---------------------------------------
    # 1. Calculate task scheduling score
    # ---------------------------------------

    def calculate_score(
        self,
        priority: int,
        sequence: int
    ) -> int:

        if not 0 <= priority <= self.MAX_PRIORITY:

            raise ValueError(
                "Priority must be between 0 and 9"
            )

        return (
            (self.MAX_PRIORITY - priority)
            * self.PRIORITY_BASE
            + sequence
        )

    # ---------------------------------------
    # 2. Add task to ready queue
    # ---------------------------------------

    def enqueue(
        self,
        task_id: str,
        priority: int
    ) -> int:

        if not 0 <= priority <= self.MAX_PRIORITY:

            raise ValueError(
                "Priority must be between 0 and 9"
            )

        # Generate unique enqueue sequence
        sequence = self.redis.incr(
            self.sequence_key
        )

        # Calculate scheduling score
        score = self.calculate_score(
            priority,
            sequence
        )

        # Insert task ID into sorted set
        self.redis.zadd(
            self.queue_key,
            {task_id: score}
        )

        return sequence

    # ---------------------------------------
    # 3. Peek at next task without removing it
    # ---------------------------------------

    def peek_next_task(self):

        result = self.redis.zrange(
            self.queue_key,
            0,
            0
        )

        if not result:
            return None

        return result[0]

    # ---------------------------------------
    # 4. Get all tasks in execution order
    # ---------------------------------------

    def get_all_tasks(self):

        return self.redis.zrange(
            self.queue_key,
            0,
            -1
        )

    # ---------------------------------------
    # 5. Remove a task from ready queue
    # ---------------------------------------

    def remove_task(self, task_id: str) -> bool:

        removed = self.redis.zrem(
            self.queue_key,
            task_id
        )

        return removed == 1

    # ---------------------------------------
    # 6. ATOMIC DEQUEUE
    # ---------------------------------------

    def dequeue(self):

        # ZPOPMIN atomically:
        # 1. Finds the lowest-scored task.
        # 2. Removes it from the sorted set.
        # 3. Returns the task ID and its score.

        result = self.redis.zpopmin(
            self.queue_key,
            count=1
        )

        # Queue is empty
        if not result:
            return None

        # Extract task ID
        task_id, score = result[0]

        # Handle both bytes and string responses
        if isinstance(task_id, bytes):

            task_id = task_id.decode("utf-8")

        return task_id

    # ---------------------------------------
    # 7. Get ready queue size
    # ---------------------------------------

    def get_queue_size(self) -> int:

        return self.redis.zcard(
            self.queue_key
        )

    # ---------------------------------------
    # 8. Check if task exists in queue
    # ---------------------------------------

    def contains(self, task_id: str) -> bool:

        return self.redis.zscore(
            self.queue_key,
            task_id
        ) is not None

    # ---------------------------------------
    # 9. Get task scheduling score
    # ---------------------------------------

    def get_task_score(self, task_id: str):

        return self.redis.zscore(
            self.queue_key,
            task_id
        )
