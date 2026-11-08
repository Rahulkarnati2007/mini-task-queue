
from datetime import datetime, timezone
import time


class DelayedQueue:

    PRIORITY_BASE = 10**12
    MAX_PRIORITY = 9

    def __init__(
        self,
        redis_client,
        queue_key="tasks:delayed"
    ):
        self.redis = redis_client
        self.queue_key = queue_key

    def enqueue(self, task_id: str, retry_at: datetime):
        timestamp = retry_at.timestamp()

        self.redis.zadd(
            self.queue_key,
            {task_id: timestamp}
        )

        return timestamp

    def get_due_tasks(self, limit=100):
        now = datetime.now(timezone.utc).timestamp()

        return self.redis.zrangebyscore(
            self.queue_key,
            "-inf",
            now,
            start=0,
            num=limit
        )

    def remove_task(self, task_id: str) -> bool:
        return self.redis.zrem(
            self.queue_key,
            task_id
        ) == 1

    def get_queue_size(self) -> int:
        return self.redis.zcard(self.queue_key)

    def get_retry_at(self, task_id: str):
        timestamp = self.redis.zscore(
            self.queue_key,
            task_id
        )

        if timestamp is None:
            return None

        return datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc
        )

    def transition_to_delayed(
        self,
        task_id: str,
        lease_token: str,
        updates: dict,
        retry_timestamp: float,
    ):
        """
        Atomically:
        1. Verify worker lease ownership.
        2. Verify task is FAILED.
        3. Update task fields and state to DELAYED.
        4. Insert task into delayed queue.
        5. Remove processing reservation and lease data.
        """

        task_key = f"task:{task_id}"
        lease_key = "tasks:processing:leases"
        processing_key = "tasks:processing"
        owner_key = "tasks:processing:owners"

        lua_script = """
        if redis.call('EXISTS', KEYS[1]) == 0 then
            return 0
        end

        local token = redis.call('HGET', KEYS[2], ARGV[1])

        if not token or token ~= ARGV[2] then
            return -1
        end

        local state = redis.call('HGET', KEYS[1], 'state')

        if state ~= 'failed' then
            return -2
        end

        local count = tonumber(ARGV[4])
        local index = 5

        for i = 1, count do
            redis.call(
                'HSET',
                KEYS[1],
                ARGV[index],
                ARGV[index + 1]
            )
            index = index + 2
        end

        redis.call(
            'ZADD',
            KEYS[5],
            ARGV[3],
            ARGV[1]
        )

        redis.call('ZREM', KEYS[3], ARGV[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
        redis.call('HDEL', KEYS[4], ARGV[1])

        return 1
        """

        flattened_updates = []

        for field, value in updates.items():
            flattened_updates.extend([
                field,
                "" if value is None else str(value),
            ])

        result = self.redis.eval(
            lua_script,
            5,
            task_key,
            lease_key,
            processing_key,
            owner_key,
            self.queue_key,
            task_id,
            lease_token,
            retry_timestamp,
            len(updates),
            *flattened_updates,
        )

        return int(result)

    def transition_to_pending(
        self,
        task_id: str,
        updates: dict,
        ready_queue_key="tasks:ready",
        sequence_key="tasks:ready:sequence",
        now_timestamp=None,
    ):
        """
        Atomically:
        1. Verify that the task exists and is DELAYED.
        2. Verify its retry time has arrived.
        3. Update its state and metadata to PENDING.
        4. Add it to the ready queue using its priority.
        5. Remove it from the delayed queue.

        Returns:
            1  = success
            0  = task does not exist
           -1  = task is not DELAYED
           -2  = retry time has not arrived
           -3  = task has no delayed queue entry
           -4  = invalid task priority
        """

        if now_timestamp is None:
            now_timestamp = time.time()

        task_key = f"task:{task_id}"

        lua_script = """
        if redis.call('EXISTS', KEYS[1]) == 0 then
            return 0
        end

        local state = redis.call('HGET', KEYS[1], 'state')

        if state ~= 'delayed' then
            return -1
        end

        local retry_timestamp = redis.call(
            'ZSCORE',
            KEYS[2],
            ARGV[1]
        )

        if not retry_timestamp then
            return -3
        end

        if tonumber(retry_timestamp) > tonumber(ARGV[2]) then
            return -2
        end

        local priority = tonumber(
            redis.call('HGET', KEYS[1], 'priority')
        )

        if not priority or priority < 0 or priority > 9 then
            return -4
        end

        local count = tonumber(ARGV[3])
        local index = 4

        for i = 1, count do
            redis.call(
                'HSET',
                KEYS[1],
                ARGV[index],
                ARGV[index + 1]
            )
            index = index + 2
        end

        local sequence = redis.call('INCR', KEYS[4])

        local score = (9 - priority) * 1000000000000 + sequence

        redis.call(
            'ZADD',
            KEYS[3],
            score,
            ARGV[1]
        )

        redis.call(
            'ZREM',
            KEYS[2],
            ARGV[1]
        )

        return 1
        """

        flattened_updates = []

        for field, value in updates.items():
            flattened_updates.extend([
                field,
                "" if value is None else str(value),
            ])

        result = self.redis.eval(
            lua_script,
            4,
            task_key,
            self.queue_key,
            ready_queue_key,
            sequence_key,
            task_id,
            now_timestamp,
            len(updates),
            *flattened_updates,
        )

        return int(result)
