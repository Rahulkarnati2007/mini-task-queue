
import pytest
import uuid

from app.core.task import TaskCreate, TaskState
from app.core.task_service import TaskService

from app.storage.redis_client import get_redis_client
from app.storage.task_storage import TaskStorage
from app.storage.ready_queue import ReadyQueue


@pytest.fixture
def service():

    redis_client = get_redis_client()

    assert redis_client.ping()

    storage = TaskStorage(redis_client)

    queue_key = f"test:service:{uuid.uuid4()}"

    queue = ReadyQueue(
        redis_client,
        queue_key=queue_key
    )

    task_service = TaskService(
        task_storage=storage,
        ready_queue=queue
    )

    created_task_ids = []

    yield task_service, created_task_ids

    # Clean up all tasks created during the test
    for task_id in created_task_ids:
        storage.delete_task(task_id)

    redis_client.delete(
        queue_key,
        queue.sequence_key
    )


def test_submit_task(service):

    task_service, created_ids = service

    request = TaskCreate(
        task_type="send_email",
        payload={"to": "rahul@example.com"},
        priority=8
    )

    task = task_service.submit_task(request)

    created_ids.append(task.id)

    assert task.id is not None
    assert task.state == TaskState.PENDING
    assert task.priority == 8

    assert task_service.get_queue_size() == 1


def test_task_is_saved_in_redis(service):

    task_service, created_ids = service

    request = TaskCreate(
        task_type="send_email"
    )

    task = task_service.submit_task(request)

    created_ids.append(task.id)

    retrieved = task_service.get_task(task.id)

    assert retrieved is not None
    assert retrieved.id == task.id
    assert retrieved.task_type == "send_email"


def test_task_is_added_to_ready_queue(service):

    task_service, created_ids = service

    request = TaskCreate(
        task_type="process_image",
        priority=9
    )

    task = task_service.submit_task(request)

    created_ids.append(task.id)

    ready_tasks = task_service.get_ready_tasks()

    assert len(ready_tasks) == 1
    assert ready_tasks[0].id == task.id


def test_priority_ordering(service):

    task_service, created_ids = service

    low = task_service.submit_task(
        TaskCreate(
            task_type="low_priority",
            priority=2
        )
    )

    medium = task_service.submit_task(
        TaskCreate(
            task_type="medium_priority",
            priority=5
        )
    )

    high = task_service.submit_task(
        TaskCreate(
            task_type="high_priority",
            priority=9
        )
    )

    created_ids.extend([
        low.id,
        medium.id,
        high.id
    ])

    ready_tasks = task_service.get_ready_tasks()

    assert ready_tasks[0].id == high.id
    assert ready_tasks[1].id == medium.id
    assert ready_tasks[2].id == low.id


def test_fifo_ordering(service):

    task_service, created_ids = service

    first = task_service.submit_task(
        TaskCreate(
            task_type="task_one",
            priority=5
        )
    )

    second = task_service.submit_task(
        TaskCreate(
            task_type="task_two",
            priority=5
        )
    )

    created_ids.extend([first.id, second.id])

    ready_tasks = task_service.get_ready_tasks()

    assert ready_tasks[0].id == first.id
    assert ready_tasks[1].id == second.id


def test_get_queue_size(service):

    task_service, created_ids = service

    assert task_service.get_queue_size() == 0

    for i in range(3):

        task = task_service.submit_task(
            TaskCreate(
                task_type=f"task_{i}"
            )
        )

        created_ids.append(task.id)

    assert task_service.get_queue_size() == 3


def test_get_nonexistent_task(service):

    task_service, created_ids = service

    result = task_service.get_task(
        str(uuid.uuid4())
    )

    assert result is None
