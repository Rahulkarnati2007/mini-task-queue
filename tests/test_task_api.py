
import pytest
import uuid

from app.core.task import TaskState
from fastapi.testclient import TestClient

from app.main import app
from app.api.dependencies import get_task_service

from app.storage.redis_client import get_redis_client
from app.storage.task_storage import TaskStorage
from app.storage.ready_queue import ReadyQueue

from app.core.task_service import TaskService


@pytest.fixture
def api_client():

    redis_client = get_redis_client()

    assert redis_client.ping()

    storage = TaskStorage(redis_client)

    queue_key = f"test:api:{uuid.uuid4()}"

    queue = ReadyQueue(
        redis_client,
        queue_key=queue_key
    )

    service = TaskService(
        task_storage=storage,
        ready_queue=queue
    )

    created_ids = []

    app.dependency_overrides[get_task_service] = (
        lambda: service
    )

    with TestClient(app) as client:

        yield client, created_ids, storage, queue

    # Clean up test data
    for task_id in created_ids:

        queue.remove_task(task_id)
        storage.delete_task(task_id)

    redis_client.delete(
        queue_key,
        queue.sequence_key
    )

    app.dependency_overrides.pop(
        get_task_service,
        None
    )


# ---------------------------------------
# 1. Successful task submission
# ---------------------------------------

def test_submit_task(api_client):

    client, created_ids, storage, queue = api_client

    response = client.post(
        "/tasks",
        json={
            "task_type": "send_email",
            "payload": {
                "to": "rahul@example.com"
            },
            "priority": 8,
            "max_attempts": 3
        }
    )

    assert response.status_code == 201

    data = response.json()

    assert data["task_type"] == "send_email"
    assert data["priority"] == 8
    assert data["state"] == "pending"

    created_ids.append(data["task_id"])


# ---------------------------------------
# 2. Verify task saved in Redis
# ---------------------------------------

def test_task_saved_in_redis(api_client):

    client, created_ids, storage, queue = api_client

    response = client.post(
        "/tasks",
        json={
            "task_type": "process_image",
            "payload": {"image": "photo.jpg"},
            "priority": 7
        }
    )

    assert response.status_code == 201

    task_id = response.json()["task_id"]

    created_ids.append(task_id)

    task = storage.get_task(task_id)

    assert task is not None
    assert task.task_type == "process_image"


# ---------------------------------------
# 3. Verify task in ready queue
# ---------------------------------------

def test_task_added_to_ready_queue(api_client):

    client, created_ids, storage, queue = api_client

    response = client.post(
        "/tasks",
        json={
            "task_type": "generate_report",
            "priority": 9
        }
    )

    assert response.status_code == 201

    task_id = response.json()["task_id"]

    created_ids.append(task_id)

    assert queue.contains(task_id)


# ---------------------------------------
# 4. Invalid priority
# ---------------------------------------

def test_invalid_priority(api_client):

    client, created_ids, storage, queue = api_client

    response = client.post(
        "/tasks",
        json={
            "task_type": "send_email",
            "priority": 15
        }
    )

    assert response.status_code == 422


# ---------------------------------------
# 5. Missing task type
# ---------------------------------------

def test_missing_task_type(api_client):

    client, created_ids, storage, queue = api_client

    response = client.post(
        "/tasks",
        json={
            "payload": {"to": "rahul@example.com"}
        }
    )

    assert response.status_code == 422


# ---------------------------------------
# 6. Successfully retrieve task
# ---------------------------------------

def test_get_task(api_client):

    client, created_ids, storage, queue = api_client

    submit_response = client.post(
        "/tasks",
        json={
            "task_type": "send_email",
            "payload": {
                "to": "rahul@example.com",
                "subject": "Testing GET API"
            },
            "priority": 9,
            "max_attempts": 3
        }
    )

    assert submit_response.status_code == 201

    task_id = submit_response.json()["task_id"]

    created_ids.append(task_id)

    response = client.get(f"/tasks/{task_id}")

    assert response.status_code == 200

    data = response.json()

    assert data["task_id"] == task_id
    assert data["task_type"] == "send_email"
    assert data["priority"] == 9
    assert data["state"] == "pending"
    assert data["attempts"] == 0
    assert data["max_attempts"] == 3

    assert data["payload"]["to"] == "rahul@example.com"


# ---------------------------------------
# 7. Retrieve nonexistent task
# ---------------------------------------

def test_get_nonexistent_task(api_client):

    client, created_ids, storage, queue = api_client

    fake_id = str(uuid.uuid4())

    response = client.get(f"/tasks/{fake_id}")

    assert response.status_code == 404


# ---------------------------------------
# 8. List pending tasks
# ---------------------------------------

def test_list_tasks(api_client):

    client, created_ids, storage, queue = api_client

    # Submit three tasks
    priorities = [5, 9, 2]

    for priority in priorities:

        response = client.post(
            "/tasks",
            json={
                "task_type": f"task_priority_{priority}",
                "priority": priority
            }
        )

        assert response.status_code == 201

        created_ids.append(
            response.json()["task_id"]
        )

    # Retrieve task list
    response = client.get("/tasks")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 3
    assert len(data["tasks"]) == 3

    # Highest priority should appear first
    returned_priorities = [
        task["priority"]
        for task in data["tasks"]
    ]

    assert returned_priorities == [9, 5, 2]


# ---------------------------------------
# 9. Test task listing limit
# ---------------------------------------

def test_list_tasks_with_limit(api_client):

    client, created_ids, storage, queue = api_client

    for i in range(5):

        response = client.post(
            "/tasks",
            json={
                "task_type": f"task_{i}",
                "priority": 5
            }
        )

        assert response.status_code == 201

        created_ids.append(
            response.json()["task_id"]
        )

    response = client.get("/tasks?limit=2")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 5
    assert data["limit"] == 2
    assert len(data["tasks"]) == 2


# ---------------------------------------
# 10. Test empty queue
# ---------------------------------------

def test_list_empty_queue(api_client):

    client, created_ids, storage, queue = api_client

    response = client.get("/tasks")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 0
    assert data["tasks"] == []


# ---------------------------------------
# 11. Successfully cancel pending task
# ---------------------------------------

def test_cancel_pending_task(api_client):

    client, created_ids, storage, queue = api_client

    # Submit a task
    submit_response = client.post(
        "/tasks",
        json={
            "task_type": "generate_report",
            "priority": 8
        }
    )

    assert submit_response.status_code == 201

    task_id = submit_response.json()["task_id"]

    created_ids.append(task_id)

    # Verify task is initially in ready queue
    assert queue.contains(task_id)

    # Cancel the task
    response = client.delete(
        f"/tasks/{task_id}"
    )

    assert response.status_code == 200

    data = response.json()

    assert data["task_id"] == task_id
    assert data["state"] == "cancelled"

    # Verify task is removed from ready queue
    assert not queue.contains(task_id)

    # Verify task still exists in Redis
    task = storage.get_task(task_id)

    assert task is not None

    # Verify updated state
    assert task.state == TaskState.CANCELLED

    # Verify completion timestamp
    assert task.completed_at is not None


# ---------------------------------------
# 12. Cancel nonexistent task
# ---------------------------------------

def test_cancel_nonexistent_task(api_client):

    client, created_ids, storage, queue = api_client

    fake_id = str(uuid.uuid4())

    response = client.delete(
        f"/tasks/{fake_id}"
    )

    assert response.status_code == 404

    assert response.json()["detail"] == (
        f"Task {fake_id} not found"
    )


# ---------------------------------------
# 13. Cannot cancel running task
# ---------------------------------------

def test_cancel_running_task(api_client):

    client, created_ids, storage, queue = api_client

    # Submit task
    submit_response = client.post(
        "/tasks",
        json={
            "task_type": "process_image",
            "priority": 7
        }
    )

    assert submit_response.status_code == 201

    task_id = submit_response.json()["task_id"]

    created_ids.append(task_id)

    # Simulate a worker having claimed the task
    queue.remove_task(task_id)

    task = storage.get_task(task_id)

    task.state = TaskState.RUNNING

    storage.update_task(task)

    # Attempt cancellation
    response = client.delete(
        f"/tasks/{task_id}"
    )

    assert response.status_code == 409

    # Verify task remains running
    updated_task = storage.get_task(task_id)

    assert updated_task.state == TaskState.RUNNING
