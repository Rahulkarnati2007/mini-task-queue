import time


class DeadLetterQueue:

    def __init__(
        self,
        redis_client,
        queue_key="tasks:dead"
    ):
        self.redis = redis_client
        self.queue_key = queue_key

    def add_task(self, task_id: str) -> bool:
        self.redis.zadd(
            self.queue_key,
            {task_id: time.time()}
        )
        return True

    def get_all_tasks(self):
        return self.redis.zrange(
            self.queue_key,
            0,
            -1
        )

    def remove_task(self, task_id: str) -> bool:
        return self.redis.zrem(
            self.queue_key,
            task_id
        ) == 1

    def contains(self, task_id: str) -> bool:
        return self.redis.zscore(
            self.queue_key,
            task_id
        ) is not None

    def get_queue_size(self) -> int:
        return self.redis.zcard(self.queue_key)
    
    
    def transition_to_dead(
        self,
        task_id: str,
        lease_token: str,
        updates: dict,
    ):
        """
        Atomically:
        1. Verify worker lease ownership.
        2. Verify task is FAILED.
        3. Update task state and metadata to DEAD.
        4. Insert task into DLQ.
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

        redis.call(
            'ZADD',
            KEYS[5],
            ARGV[1] == '' and 0 or ARGV[1],
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
            len(updates),
            *flattened_updates,
        )

        return int(result)
