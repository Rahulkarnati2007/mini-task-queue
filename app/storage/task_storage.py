import json

from app.core.task import Task, current_timestamp


class TaskStorage:

    def __init__(self, redis_client):
        self.redis = redis_client

    def serialize_task(self, task: Task) -> dict:
        data = task.model_dump(mode="json")
        data["payload"] = json.dumps(data["payload"])

        for key, value in data.items():
            if value is None:
                data[key] = ""
            elif key != "payload":
                data[key] = str(value)

        return data

    def deserialize_task(self, data: dict) -> Task:
        if not data:
            return None

        data = dict(data)
        data["payload"] = json.loads(data["payload"])

        data["priority"] = int(data["priority"])
        data["attempts"] = int(data["attempts"])
        data["max_attempts"] = int(data["max_attempts"])

        optional_fields = [
            "idempotency_key",
            "started_at",
            "completed_at",
            "retry_at",
            "last_error"
        ]

        for field in optional_fields:
            if data.get(field, "") == "":
                data[field] = None

        return Task.model_validate(data)

    def save_task(self, task: Task) -> bool:
        key = f"task:{task.id}"

        serialized_data = self.serialize_task(task)

        self.redis.hset(
            key,
            mapping=serialized_data
        )

        return True

    def get_task(self, task_id: str):

        key = f"task:{task_id}"
        data = self.redis.hgetall(key)

        if not data:
            return None

        return self.deserialize_task(data)

    def update_task(self, task: Task) -> bool:

        key = f"task:{task.id}"

        if not self.redis.exists(key):
            return False

        task.updated_at = current_timestamp()

        serialized_data = self.serialize_task(task)

        self.redis.hset(
            key,
            mapping=serialized_data
        )

        return True

    def delete_task(self, task_id: str) -> bool:

        key = f"task:{task_id}"
        deleted = self.redis.delete(key)

        return deleted == 1

    # ---------------------------------------
    # Idempotency operations
    # ---------------------------------------

    def get_idempotency_key_name(self, idempotency_key: str):
        return f"idempotency:{idempotency_key}"

    def claim_idempotency_key(
        self,
        idempotency_key: str,
        task_id: str
    ) -> bool:

        key = self.get_idempotency_key_name(
            idempotency_key
        )

        # SET NX succeeds only if the key doesn't exist.
        result = self.redis.set(
            key,
            task_id,
            nx=True
        )

        return result is True or result == "OK"

    def get_idempotency_task_id(
        self,
        idempotency_key: str
    ):

        key = self.get_idempotency_key_name(
            idempotency_key
        )

        return self.redis.get(key)

    def release_idempotency_key(
        self,
        idempotency_key: str,
        task_id: str
    ) -> bool:

        key = self.get_idempotency_key_name(
            idempotency_key
        )

        # Delete only if this mapping still belongs
        # to the task that is being rolled back.
        lua_script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end

        return 0
        """

        result = self.redis.eval(
            lua_script,
            1,
            key,
            task_id
        )

        return result == 1
    
    
    def atomic_update_task(
        self,
        task_id: str,
        expected_state: str,
        lease_token: str,
        updates: dict,
    ) -> int:
        """
        Atomically update a task only if:
        1. The task exists.
        2. The supplied lease token is still valid.
        3. The task is in the expected state.

        Return values:
             1 = successful update
             0 = task does not exist
            -1 = lease ownership mismatch
            -2 = state mismatch
        """

        task_key = f"task:{task_id}"
        lease_key = "tasks:processing:leases"

        lua_script = """
        -- Check that the task exists.
        if redis.call('EXISTS', KEYS[1]) == 0 then
            return 0
        end

        -- Verify current lease ownership.
        local current_token = redis.call(
            'HGET',
            KEYS[2],
            ARGV[2]
        )

        if not current_token or current_token ~= ARGV[3] then
            return -1
        end

        -- Verify expected task state.
        local current_state = redis.call(
            'HGET',
            KEYS[1],
            'state'
        )

        if current_state ~= ARGV[1] then
            return -2
        end

        -- Apply all updates atomically.
        local update_count = tonumber(ARGV[4])
        local index = 5

        for i = 1, update_count do
            local field = ARGV[index]
            local value = ARGV[index + 1]

            redis.call(
                'HSET',
                KEYS[1],
                field,
                value
            )

            index = index + 2
        end

        return 1
        """

        # Convert every value to a Redis-compatible string.
        flattened_updates = []

        for field, value in updates.items():
            if value is None:
                value = ""
            else:
                value = str(value)

            flattened_updates.extend([field, value])

        result = self.redis.eval(
            lua_script,
            2,
            task_key,
            lease_key,
            expected_state,
            task_id,
            lease_token,
            len(updates),
            *flattened_updates,
        )

        return int(result)
