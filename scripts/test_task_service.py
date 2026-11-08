
from app.core.task import TaskCreate
from app.core.task_service import TaskService

from app.storage.redis_client import get_redis_client
from app.storage.task_storage import TaskStorage
from app.storage.ready_queue import ReadyQueue


redis_client = get_redis_client()

storage = TaskStorage(redis_client)

queue = ReadyQueue(
    redis_client,
    queue_key="demo:service:ready"
)

service = TaskService(
    task_storage=storage,
    ready_queue=queue
)

# Clean previous demo data
old_ids = queue.get_all_tasks()

for task_id in old_ids:
    storage.delete_task(task_id)

redis_client.delete(
    "demo:service:ready",
    "demo:service:ready:sequence"
)

# Submit three tasks
task1 = service.submit_task(
    TaskCreate(
        task_type="send_email",
        payload={"to": "user1@example.com"},
        priority=5
    )
)

task2 = service.submit_task(
    TaskCreate(
        task_type="process_image",
        payload={"image": "photo1.jpg"},
        priority=9
    )
)

task3 = service.submit_task(
    TaskCreate(
        task_type="generate_report",
        payload={"report_id": 101},
        priority=2
    )
)

print("Submitted task IDs:")
print(task1.id)
print(task2.id)
print(task3.id)

print("\nTasks in execution order:")

for task in service.get_ready_tasks():

    print(
        task.task_type,
        "| Priority:",
        task.priority,
        "| State:",
        task.state.value
    )

print("\nTotal queued tasks:", service.get_queue_size())

# Clean up
for task in [task1, task2, task3]:
    storage.delete_task(task.id)

redis_client.delete(
    "demo:service:ready",
    "demo:service:ready:sequence"
)
