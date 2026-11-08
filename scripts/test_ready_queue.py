
from app.storage.redis_client import get_redis_client
from app.storage.ready_queue import ReadyQueue


redis_client = get_redis_client()

queue = ReadyQueue(
    redis_client,
    queue_key="demo:ready"
)

# Clear previous demo data
redis_client.delete(
    "demo:ready",
    "demo:ready:sequence"
)

# Add five tasks
queue.enqueue("task101", priority=5)
queue.enqueue("task102", priority=9)
queue.enqueue("task103", priority=2)
queue.enqueue("task104", priority=5)
queue.enqueue("task105", priority=8)

# Display execution order
print("Execution order:")

for task_id in queue.get_all_tasks():
    print(task_id)

# Display total tasks
print("\nQueue size:", queue.get_queue_size())

# Display scores
print("\nScheduling scores:")

for task_id in queue.get_all_tasks():
    print(
        task_id,
        queue.get_task_score(task_id)
    )

# Clean up demo data
redis_client.delete(
    "demo:ready",
    "demo:ready:sequence"
)
