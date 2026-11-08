import time

import pytest

from app.storage.redis_client import get_redis_client
from app.storage.processing_queue import ProcessingQueue


READY_KEY = "test:tasks:ready"
PROCESSING_KEY = "test:tasks:processing"

LEASE_KEY = f"{PROCESSING_KEY}:leases"
OWNER_KEY = f"{PROCESSING_KEY}:owners"
SEQUENCE_KEY = f"{READY_KEY}:sequence"
DEAD_KEY = "test:tasks:dead"

RECOVERY_TASKS = [
    "recovery-running",
    "recovery-succeeded",
    "recovery-failed",
    "recovery-delayed",
    "recovery-dead",
    "recovery-cancelled",
    "recovery-missing",
    "recovery-invalid-priority",
    "recovery-duplicate",
    "recovery-new-lease",
    "recovery-attempts-remain",
    "recovery-attempts-exhausted",
]


@pytest.fixture
def processing_queue():

    client = get_redis_client()

    try:
        client.ping()
    except Exception:
        pytest.skip("Redis is not available")

    client.delete(
        READY_KEY,
        PROCESSING_KEY,
        LEASE_KEY,
        OWNER_KEY,
        SEQUENCE_KEY,
        DEAD_KEY,
        *[f"task:{task_id}" for task_id in RECOVERY_TASKS]
    )

    queue = ProcessingQueue(
        redis_client=client,
        ready_queue_key=READY_KEY,
        processing_queue_key=PROCESSING_KEY,
        dead_letter_queue_key=DEAD_KEY
    )

    yield queue

    client.delete(
        READY_KEY,
        PROCESSING_KEY,
        LEASE_KEY,
        OWNER_KEY,
        SEQUENCE_KEY,
        DEAD_KEY,
        *[f"task:{task_id}" for task_id in RECOVERY_TASKS]
    )

    client.close()


def test_reserve_task_moves_task_to_processing(processing_queue):

    client = processing_queue.redis

    client.zadd(READY_KEY, {"task-1": 100})

    task_id = processing_queue.reserve_task()

    assert task_id == "task-1"

    assert client.zscore(READY_KEY, "task-1") is None

    assert processing_queue.contains("task-1")


def test_reserve_task_respects_priority_order(processing_queue):

    client = processing_queue.redis

    client.zadd(
        READY_KEY,
        {
            "low-priority": 900,
            "high-priority": 100,
            "medium-priority": 500
        }
    )

    first = processing_queue.reserve_task()
    second = processing_queue.reserve_task()
    third = processing_queue.reserve_task()

    assert first == "high-priority"
    assert second == "medium-priority"
    assert third == "low-priority"


def test_reserve_task_returns_none_when_empty(processing_queue):

    result = processing_queue.reserve_task()

    assert result is None


def test_reserved_task_has_timestamp(processing_queue):

    client = processing_queue.redis

    client.zadd(READY_KEY, {"task-1": 100})

    processing_queue.reserve_task()

    timestamp = processing_queue.get_task_reserved_at("task-1")

    assert timestamp is not None
    assert timestamp > 0


def test_remove_task_from_processing(processing_queue):

    client = processing_queue.redis

    client.zadd(READY_KEY, {"task-1": 100})

    processing_queue.reserve_task()

    removed = processing_queue.remove_task("task-1")

    assert removed is True
    assert processing_queue.contains("task-1") is False


def test_processing_queue_size(processing_queue):

    client = processing_queue.redis

    client.zadd(
        READY_KEY,
        {
            "task-1": 100,
            "task-2": 200
        }
    )

    processing_queue.reserve_task()
    processing_queue.reserve_task()

    assert processing_queue.get_queue_size() == 2


# --------------------------------------------------
# Recovery tests
# --------------------------------------------------

def add_expired_task(
    processing_queue,
    task_id,
    state,
    priority="5",
    attempts=None,
    max_attempts=None
):
    """
    Create a task with an expired processing reservation.
    """

    client = processing_queue.redis

    task_key = f"task:{task_id}"

    mapping = {"state": state}

    if priority is not None:
        mapping["priority"] = str(priority)

    if attempts is not None:
        mapping["attempts"] = str(attempts)

    if max_attempts is not None:
        mapping["max_attempts"] = str(max_attempts)

    client.hset(
        task_key,
        mapping=mapping
    )

    client.zadd(
        PROCESSING_KEY,
        {task_id: time.time() - 100}
    )

    client.hset(
        LEASE_KEY,
        task_id,
        f"old-token-{task_id}"
    )

    client.hset(
        OWNER_KEY,
        task_id,
        "old-worker"
    )


def test_expired_running_task_is_recovered(processing_queue):

    client = processing_queue.redis

    task_id = "recovery-running"

    add_expired_task(
        processing_queue,
        task_id,
        state="running",
        priority=8
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id in recovered

    assert client.hget(
        f"task:{task_id}",
        "state"
    ) == "pending"

    assert processing_queue.contains(task_id) is False

    assert client.zscore(
        READY_KEY,
        task_id
    ) is not None

    assert client.hget(LEASE_KEY, task_id) is None

    assert client.hget(OWNER_KEY, task_id) is None


@pytest.mark.parametrize(
    "state",
    [
        "succeeded",
        "failed",
        "delayed",
        "dead",
        "cancelled",
    ]
)
def test_non_running_expired_task_is_not_requeued(
    processing_queue,
    state
):

    client = processing_queue.redis

    task_id = {
        "succeeded": "recovery-succeeded",
        "failed": "recovery-failed",
        "delayed": "recovery-delayed",
        "dead": "recovery-dead",
        "cancelled": "recovery-cancelled",
    }[state]

    add_expired_task(
        processing_queue,
        task_id,
        state=state,
        priority=5
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id not in recovered

    # Original state must remain unchanged.
    assert client.hget(
        f"task:{task_id}",
        "state"
    ) == state

    # Must not be reinserted into ready queue.
    assert client.zscore(
        READY_KEY,
        task_id
    ) is None

    # Stale reservation and ownership must be removed.
    assert processing_queue.contains(task_id) is False

    assert client.hget(LEASE_KEY, task_id) is None

    assert client.hget(OWNER_KEY, task_id) is None


def test_missing_task_reservation_is_cleaned_up(processing_queue):

    client = processing_queue.redis

    task_id = "recovery-missing"

    client.zadd(
        PROCESSING_KEY,
        {task_id: time.time() - 100}
    )

    client.hset(
        LEASE_KEY,
        task_id,
        "old-token"
    )

    client.hset(
        OWNER_KEY,
        task_id,
        "old-worker"
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id not in recovered

    assert processing_queue.contains(task_id) is False

    assert client.hget(LEASE_KEY, task_id) is None

    assert client.hget(OWNER_KEY, task_id) is None

    assert client.zscore(READY_KEY, task_id) is None


def test_running_task_with_invalid_priority_is_not_requeued(
    processing_queue
):

    client = processing_queue.redis

    task_id = "recovery-invalid-priority"

    add_expired_task(
        processing_queue,
        task_id,
        state="running",
        priority=15
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id not in recovered

    assert client.hget(
        f"task:{task_id}",
        "state"
    ) == "running"

    assert client.zscore(
        READY_KEY,
        task_id
    ) is None

    assert processing_queue.contains(task_id) is False

    assert client.hget(LEASE_KEY, task_id) is None

    assert client.hget(OWNER_KEY, task_id) is None


# --------------------------------------------------
# Step 32.4: Recovery edge-case tests
# --------------------------------------------------

def test_recovery_is_idempotent_when_called_twice(processing_queue):

    client = processing_queue.redis

    task_id = "recovery-duplicate"

    add_expired_task(
        processing_queue,
        task_id,
        state="running",
        priority=7
    )

    first_recovery = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    second_recovery = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id in first_recovery
    assert second_recovery == []

    # The task must appear in the ready queue exactly once.
    assert client.zcard(READY_KEY) == 1
    assert client.zscore(READY_KEY, task_id) is not None

    # It must no longer have an expired processing reservation.
    assert processing_queue.contains(task_id) is False


def test_recovered_task_gets_new_lease_and_rejects_stale_worker(
    processing_queue
):

    client = processing_queue.redis

    task_id = "recovery-new-lease"
    old_token = f"old-token-{task_id}"

    add_expired_task(
        processing_queue,
        task_id,
        state="running",
        priority=8
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id in recovered
    assert client.hget(LEASE_KEY, task_id) is None

    # A different worker reserves the recovered task.
    reserved_task_id, new_token = (
        processing_queue.reserve_task_with_lease("new-worker")
    )

    assert reserved_task_id == task_id
    assert new_token
    assert new_token != old_token

    assert processing_queue.get_task_lease(task_id) == new_token
    assert processing_queue.get_task_owner(task_id) == "new-worker"

    # The previous worker's lease is no longer valid.
    assert processing_queue.acknowledge_task(
        task_id,
        old_token
    ) is False

    # The stale acknowledgement must not remove the new reservation.
    assert processing_queue.contains(task_id) is True
    assert processing_queue.get_task_lease(task_id) == new_token

    # Only the current worker can acknowledge the task.
    assert processing_queue.acknowledge_task(
        task_id,
        new_token
    ) is True

    assert processing_queue.contains(task_id) is False
    assert processing_queue.get_task_lease(task_id) is None
    assert processing_queue.get_task_owner(task_id) is None


# --------------------------------------------------
# Step 36.2.7: Recovery attempt-limit tests
# --------------------------------------------------

def test_expired_running_task_is_requeued_when_attempts_remain(
    processing_queue
):
    client = processing_queue.redis
    task_id = "recovery-attempts-remain"

    add_expired_task(
        processing_queue,
        task_id,
        state="running",
        priority=6,
        attempts=1,
        max_attempts=3,
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id in recovered
    assert client.hget(f"task:{task_id}", "state") == "pending"
    assert client.zscore(READY_KEY, task_id) is not None
    assert client.zscore(DEAD_KEY, task_id) is None
    assert processing_queue.contains(task_id) is False
    assert client.hget(LEASE_KEY, task_id) is None
    assert client.hget(OWNER_KEY, task_id) is None


def test_expired_running_task_at_attempt_limit_moves_to_dlq_atomically(
    processing_queue
):
    client = processing_queue.redis
    task_id = "recovery-attempts-exhausted"

    add_expired_task(
        processing_queue,
        task_id,
        state="running",
        priority=6,
        attempts=1,
        max_attempts=1,
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    # DLQ tasks are not reported as requeued tasks.
    assert task_id not in recovered

    # The task is marked DEAD and appears in the DLQ.
    assert client.hget(f"task:{task_id}", "state") == "dead"
    assert client.zscore(DEAD_KEY, task_id) is not None

    # It must not be placed back in the ready queue.
    assert client.zscore(READY_KEY, task_id) is None

    # Processing and old lease ownership are removed atomically.
    assert processing_queue.contains(task_id) is False
    assert client.hget(LEASE_KEY, task_id) is None
    assert client.hget(OWNER_KEY, task_id) is None

    # A second recovery pass must not duplicate the DLQ entry.
    second_recovery = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )
    assert second_recovery == []
    assert client.zcard(DEAD_KEY) == 1


# --------------------------------------------------
# Step 36.3: Recover PENDING tasks stranded in processing
# --------------------------------------------------

def test_expired_pending_task_is_requeued_without_consuming_attempt(
    processing_queue
):
    client = processing_queue.redis
    task_id = "recovery-pending-stranded"

    add_expired_task(
        processing_queue,
        task_id,
        state="pending",
        priority=4,
        attempts=0,
        max_attempts=1,
    )

    recovered = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )

    assert task_id in recovered
    assert client.hget(f"task:{task_id}", "state") == "pending"
    assert client.hget(f"task:{task_id}", "attempts") == "0"
    assert client.zscore(READY_KEY, task_id) is not None
    assert client.zscore(DEAD_KEY, task_id) is None

    assert processing_queue.contains(task_id) is False
    assert client.hget(LEASE_KEY, task_id) is None
    assert client.hget(OWNER_KEY, task_id) is None

    second_recovery = processing_queue.recover_expired_tasks(
        visibility_timeout=30
    )
    assert second_recovery == []
    assert client.zcard(READY_KEY) == 1