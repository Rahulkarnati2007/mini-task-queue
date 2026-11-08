from app.storage.redis_client import get_redis_client
from app.storage.task_storage import TaskStorage
from app.storage.ready_queue import ReadyQueue
from app.storage.dead_letter_queue import DeadLetterQueue

from app.core.task_service import TaskService


def get_task_service() -> TaskService:

    redis_client = get_redis_client()

    task_storage = TaskStorage(redis_client)
    ready_queue = ReadyQueue(redis_client)
    dead_letter_queue = DeadLetterQueue(redis_client)

    task_service = TaskService(
        task_storage=task_storage,
        ready_queue=ready_queue,
        dead_letter_queue=dead_letter_queue
    )

    return task_service


def get_redis():
    return get_redis_client()