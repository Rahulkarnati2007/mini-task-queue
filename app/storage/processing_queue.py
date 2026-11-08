import time
import uuid
from datetime import datetime, timezone


class ProcessingQueue:
    """
    Manages tasks reserved by workers.

    Redis keys:
        tasks:processing          -> sorted set of reserved tasks
        tasks:processing:leases   -> hash of task_id -> lease token
        tasks:processing:owners   -> hash of task_id -> worker_id
    """

    def __init__(
        self,
        redis_client,
        ready_queue_key="tasks:ready",
        processing_queue_key="tasks:processing",
        dead_letter_queue_key="tasks:dead",
    ):
        self.redis = redis_client

        self.ready_queue_key = ready_queue_key
        self.processing_queue_key = processing_queue_key
        self.dead_letter_queue_key = dead_letter_queue_key

        self.lease_key = f"{processing_queue_key}:leases"
        self.owner_key = f"{processing_queue_key}:owners"

    def renew_lease(self, task_id, lease_token):
        """Renew a lease only while the supplied token owns the task."""

        lua_script = """
        local current_token = redis.call('HGET', KEYS[2], ARGV[1])

        if not current_token or current_token ~= ARGV[2] then
            return 0
        end

        if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false then
            return 0
        end

        redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
        return 1
        """

        result = self.redis.eval(
            lua_script,
            2,
            self.processing_queue_key,
            self.lease_key,
            task_id,
            lease_token,
            time.time(),
        )

        return result == 1

    # --------------------------------------------------
    # 1. Atomically reserve a task with worker ownership
    # --------------------------------------------------

    def reserve_task_with_lease(self, worker_id):
        reserved_at = time.time()
        lease_token = str(uuid.uuid4())

        lua_script = """
        local tasks = redis.call('ZRANGE', KEYS[1], 0, 0)

        if #tasks == 0 then
            return nil
        end

        local task_id = tasks[1]
        local removed = redis.call('ZREM', KEYS[1], task_id)

        if removed == 1 then
            redis.call('ZADD', KEYS[2], ARGV[1], task_id)
            redis.call('HSET', KEYS[3], task_id, ARGV[2])
            redis.call('HSET', KEYS[4], task_id, ARGV[3])
            return {task_id, ARGV[2]}
        end

        return nil
        """

        result = self.redis.eval(
            lua_script,
            4,
            self.ready_queue_key,
            self.processing_queue_key,
            self.lease_key,
            self.owner_key,
            reserved_at,
            lease_token,
            worker_id,
        )

        if result is None:
            return None

        return result[0], result[1]

    # --------------------------------------------------
    # 2. Backward-compatible reservation method
    # --------------------------------------------------

    def reserve_task(self):
        """Legacy method returning only the task ID."""
        result = self.reserve_task_with_lease("legacy-worker")

        if result is None:
            return None

        task_id, _ = result
        return task_id

    # --------------------------------------------------
    # 3. Safely acknowledge a task
    # --------------------------------------------------

    def acknowledge_task(self, task_id, lease_token):
        """Acknowledge only if the supplied token owns the reservation."""

        lua_script = """
        local current_token = redis.call('HGET', KEYS[2], ARGV[1])

        if not current_token or current_token ~= ARGV[2] then
            return 0
        end

        redis.call('ZREM', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
        redis.call('HDEL', KEYS[3], ARGV[1])

        return 1
        """

        result = self.redis.eval(
            lua_script,
            3,
            self.processing_queue_key,
            self.lease_key,
            self.owner_key,
            task_id,
            lease_token,
        )

        return result == 1

    # --------------------------------------------------
    # 4. Check current lease ownership
    # --------------------------------------------------

    def is_lease_owner(self, task_id, lease_token):
        if lease_token is None:
            return False

        current_token = self.redis.hget(self.lease_key, task_id)
        return current_token == lease_token

    # --------------------------------------------------
    # 5. Get lease token and worker owner
    # --------------------------------------------------

    def get_task_lease(self, task_id):
        return self.redis.hget(self.lease_key, task_id)

    def get_task_owner(self, task_id):
        return self.redis.hget(self.owner_key, task_id)

    # --------------------------------------------------
    # 6. Get all processing tasks
    # --------------------------------------------------

    def get_all_tasks(self):
        return self.redis.zrange(self.processing_queue_key, 0, -1)

    # --------------------------------------------------
    # 7. Get processing tasks with timestamps
    # --------------------------------------------------

    def get_all_tasks_with_scores(self):
        return self.redis.zrange(
            self.processing_queue_key,
            0,
            -1,
            withscores=True,
        )

    # --------------------------------------------------
    # 8. Legacy unconditional removal
    # --------------------------------------------------

    def remove_task(self, task_id: str) -> bool:
        """
        Administrative removal method.

        Workers should use acknowledge_task() instead.
        """
        removed = self.redis.zrem(self.processing_queue_key, task_id)
        self.redis.hdel(self.lease_key, task_id)
        self.redis.hdel(self.owner_key, task_id)

        return removed == 1

    # --------------------------------------------------
    # 9. Check whether task is processing
    # --------------------------------------------------

    def contains(self, task_id: str) -> bool:
        return (
            self.redis.zscore(self.processing_queue_key, task_id)
            is not None
        )

    # --------------------------------------------------
    # 10. Processing queue size
    # --------------------------------------------------

    def get_queue_size(self) -> int:
        return self.redis.zcard(self.processing_queue_key)

    # --------------------------------------------------
    # 11. Get reservation timestamp
    # --------------------------------------------------

    def get_task_reserved_at(self, task_id):
        return self.redis.zscore(self.processing_queue_key, task_id)

    # --------------------------------------------------
    # 12. Recover expired tasks with attempt-limit handling
    # --------------------------------------------------

    def recover_expired_tasks(self, visibility_timeout=30, batch_size=100):
        """
        Atomically recover expired RUNNING and stranded PENDING reservations.

        If attempts < max_attempts, an expired RUNNING task returns to the ready
        queue as PENDING. An already-PENDING task is requeued without consuming
        another attempt.

        If attempts >= max_attempts, the task becomes DEAD and is added
        to the DLQ.

        In both cases, the old processing reservation, lease, and owner
        are removed in the same Redis Lua operation.

        Returns:
            List of task IDs requeued to the ready queue.
            Tasks moved to the DLQ are intentionally not in this list.
        """

        now = datetime.now(timezone.utc)
        cutoff_time = now.timestamp() - visibility_timeout
        now_timestamp = now.timestamp()
        now_iso = now.isoformat()

        lua_script = """
        local expired_tasks = redis.call(
            'ZRANGEBYSCORE',
            KEYS[1],
            '-inf',
            ARGV[1],
            'LIMIT',
            0,
            ARGV[2]
        )

        local recovered = {}

        for _, task_id in ipairs(expired_tasks) do
            local task_key = 'task:' .. task_id

            local state = redis.call('HGET', task_key, 'state')
            local priority = redis.call('HGET', task_key, 'priority')

            if (state == 'running' or state == 'pending') and priority then
                priority = tonumber(priority)

                if priority and priority >= 0 and priority <= 9 then
                    if state == 'running' then
                        local attempts = tonumber(
                            redis.call('HGET', task_key, 'attempts')
                        ) or 0

                        local max_attempts = tonumber(
                            redis.call('HGET', task_key, 'max_attempts')
                        ) or 3

                        if attempts >= max_attempts then
                            -- Attempt limit exhausted: atomically move to DLQ.
                            redis.call(
                                'HSET',
                                task_key,
                                'state', 'dead',
                                'updated_at', ARGV[4],
                                'completed_at', ARGV[4]
                            )

                            redis.call(
                                'ZADD',
                                KEYS[6],
                                ARGV[3],
                                task_id
                            )
                        else
                            -- Attempts remain: return RUNNING task to PENDING.
                            local sequence = redis.call('INCR', KEYS[4])

                            local score =
                                (9 - priority) * 1000000000000 + sequence

                            redis.call(
                                'HSET',
                                task_key,
                                'state', 'pending',
                                'updated_at', ARGV[4]
                            )

                            redis.call('HDEL', task_key, 'completed_at')

                            redis.call(
                                'ZADD',
                                KEYS[2],
                                score,
                                task_id
                            )

                            table.insert(recovered, task_id)
                        end
                    else
                        -- A PENDING task was reserved but never transitioned
                        -- to RUNNING. Requeue it without consuming an attempt.
                        local sequence = redis.call('INCR', KEYS[4])

                        local score =
                            (9 - priority) * 1000000000000 + sequence

                        redis.call(
                            'HSET',
                            task_key,
                            'updated_at', ARGV[4]
                        )

                        redis.call(
                            'ZADD',
                            KEYS[2],
                            score,
                            task_id
                        )

                        table.insert(recovered, task_id)
                    end
                end
            end

            -- Always clear the expired reservation and old ownership.
            redis.call('ZREM', KEYS[1], task_id)
            redis.call('HDEL', KEYS[3], task_id)
            redis.call('HDEL', KEYS[5], task_id)
        end

        return recovered
        """

        recovered_tasks = self.redis.eval(
            lua_script,
            6,
            self.processing_queue_key,
            self.ready_queue_key,
            self.lease_key,
            f"{self.ready_queue_key}:sequence",
            self.owner_key,
            self.dead_letter_queue_key,
            cutoff_time,
            batch_size,
            now_timestamp,
            now_iso,
        )

        return recovered_tasks
