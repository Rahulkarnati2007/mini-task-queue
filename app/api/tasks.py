
from datetime import datetime
from typing import Any, Optional
from app.core.queue_stats import QueueStatsService
from app.core.worker_metrics import WorkerMetrics
from app.api.dependencies import get_task_service, get_redis
import redis
from app.storage.dead_letter_queue import DeadLetterQueue
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    status
)

from pydantic import BaseModel

from app.core.task import TaskCreate

from app.core.state_manager import (
    TaskNotFound,
    InvalidStateTransition
)

from app.core.task_service import TaskService

from app.api.dependencies import get_task_service


router = APIRouter()


# ---------------------------------------
# Response: Task submission
# ---------------------------------------

class TaskSubmissionResponse(BaseModel):

    task_id: str
    task_type: str
    priority: int
    state: str
    message: str


# ---------------------------------------
# Response: Task details
# ---------------------------------------

class TaskDetailsResponse(BaseModel):

    task_id: str
    task_type: str
    payload: dict[str, Any]
    priority: int
    state: str

    attempts: int
    max_attempts: int

    created_at: datetime
    updated_at: datetime

    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    last_error: Optional[str] = None
    idempotency_key: Optional[str] = None


# ---------------------------------------
# Response: Task listing
# ---------------------------------------

class TaskListResponse(BaseModel):

    total: int
    limit: int
    tasks: list[TaskDetailsResponse]


# ---------------------------------------
# Response: Task cancellation
# ---------------------------------------

class TaskCancellationResponse(BaseModel):

    task_id: str
    state: str
    message: str

class DeadLetterQueueResponse(BaseModel):
    total: int
    tasks: list[TaskDetailsResponse]


class DLQRequeueResponse(BaseModel):
    task_id: str
    state: str
    attempts: int
    max_attempts: int
    message: str
    
    
class QueueStatsResponse(BaseModel):
    total_tasks: int
    queues: dict[str, int]
    states: dict[str, int]
    active_tasks: int


class WorkerMetricsResponse(BaseModel):
    worker_id: str
    status: str
    tasks_processed: int
    tasks_succeeded: int
    tasks_failed: int
    total_execution_time: float
    average_execution_time: float
    last_execution_time: float
    started_at: float
    stopped_at: float
    updated_at: float


class WorkerMetricsListResponse(BaseModel):
    total_workers: int
    workers: list[WorkerMetricsResponse]
    
# ---------------------------------------
# Helper: Convert task to response
# ---------------------------------------

def convert_task(task):

    return TaskDetailsResponse(
        task_id=task.id,
        task_type=task.task_type,
        payload=task.payload,
        priority=task.priority,
        state=task.state.value,
        attempts=task.attempts,
        max_attempts=task.max_attempts,
        created_at=task.created_at,
        updated_at=task.updated_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        last_error=task.last_error,
        idempotency_key=task.idempotency_key
    )


# ---------------------------------------
# POST /tasks
# ---------------------------------------

@router.post(
    "/tasks",
    response_model=TaskSubmissionResponse,
    status_code=status.HTTP_201_CREATED
)
def submit_task(
    request: TaskCreate,
    task_service: TaskService = Depends(get_task_service)
):

    try:

        task = task_service.submit_task(request)

        return TaskSubmissionResponse(
            task_id=task.id,
            task_type=task.task_type,
            priority=task.priority,
            state=task.state.value,
            message="Task submitted successfully"
        )

    except redis.RedisError:

        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )


# ---------------------------------------
# GET /tasks
# ---------------------------------------

@router.get(
    "/tasks",
    response_model=TaskListResponse
)
def list_tasks(
    limit: int = Query(default=20, ge=1, le=100),
    task_service: TaskService = Depends(get_task_service)
):

    try:

        ready_tasks = task_service.get_ready_tasks()

        total = len(ready_tasks)

        selected_tasks = ready_tasks[:limit]

        task_responses = [
            convert_task(task)
            for task in selected_tasks
        ]

        return TaskListResponse(
            total=total,
            limit=limit,
            tasks=task_responses
        )

    except redis.RedisError:

        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )


# ---------------------------------------
# DELETE /tasks/{task_id}
# ---------------------------------------

@router.delete(
    "/tasks/{task_id}",
    response_model=TaskCancellationResponse
)
def cancel_task(
    task_id: str,
    task_service: TaskService = Depends(get_task_service)
):

    try:

        task = task_service.cancel_task(task_id)

        return TaskCancellationResponse(
            task_id=task.id,
            state=task.state.value,
            message="Task cancelled successfully"
        )

    except TaskNotFound:

        raise HTTPException(
            status_code=404,
            detail=f"Task {task_id} not found"
        )

    except InvalidStateTransition as error:

        raise HTTPException(
            status_code=409,
            detail=str(error)
        )

    except redis.RedisError:

        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )


# ---------------------------------------
# GET /tasks/{task_id}
# ---------------------------------------

@router.get(
    "/tasks/{task_id}",
    response_model=TaskDetailsResponse
)
def get_task(
    task_id: str,
    task_service: TaskService = Depends(get_task_service)
):

    try:

        task = task_service.get_task(task_id)

        if task is None:

            raise HTTPException(
                status_code=404,
                detail=f"Task {task_id} not found"
            )

        return convert_task(task)

    except redis.RedisError:

        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )

# ---------------------------------------
# GET /dlq
# ---------------------------------------

@router.get(
    "/dlq",
    response_model=DeadLetterQueueResponse
)
def get_dead_letter_tasks(
    task_service: TaskService = Depends(get_task_service)
):

    try:
        tasks = task_service.get_dead_tasks()

        return DeadLetterQueueResponse(
            total=len(tasks),
            tasks=[
                convert_task(task)
                for task in tasks
            ]
        )

    except redis.RedisError:
        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )


# ---------------------------------------
# POST /dlq/{task_id}/requeue
# ---------------------------------------

@router.post(
    "/dlq/{task_id}/requeue",
    response_model=DLQRequeueResponse
)
def requeue_dead_task(
    task_id: str,
    task_service: TaskService = Depends(get_task_service)
):

    try:
        task = task_service.requeue_dead_task(task_id)

        return DLQRequeueResponse(
            task_id=task.id,
            state=task.state.value,
            attempts=task.attempts,
            max_attempts=task.max_attempts,
            message="Task successfully requeued"
        )

    except TaskNotFound:
        raise HTTPException(
            status_code=404,
            detail=f"Task {task_id} not found"
        )

    except InvalidStateTransition as error:
        raise HTTPException(
            status_code=409,
            detail=str(error)
        )

    except redis.RedisError:
        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )
    # ---------------------------------------
# GET /stats
# ---------------------------------------

@router.get(
    "/stats",
    response_model=QueueStatsResponse
)
def get_queue_stats(
    redis_client=Depends(get_redis)
):

    try:

        stats_service = QueueStatsService(redis_client)

        stats = stats_service.get_queue_stats()

        return QueueStatsResponse(**stats)

    except redis.RedisError:

        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )

# ---------------------------------------
# GET /workers/metrics
# ---------------------------------------

@router.get(
    "/workers/metrics",
    response_model=WorkerMetricsListResponse
)
def get_worker_metrics(
    redis_client=Depends(get_redis)
):
    try:
        metrics_service = WorkerMetrics(redis_client)
        workers = metrics_service.get_all_worker_metrics()

        return WorkerMetricsListResponse(
            total_workers=len(workers),
            workers=workers
        )

    except redis.RedisError:
        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )

# ---------------------------------------
# Response: Worker health
# ---------------------------------------

class WorkerHealthResponse(BaseModel):
    worker_id: str
    status: str
    health: str
    last_heartbeat_at: Optional[float] = None
    heartbeat_age_seconds: Optional[float] = None
    stale_after_seconds: float


class WorkerHealthListResponse(BaseModel):
    total_workers: int
    workers: list[WorkerHealthResponse]


# ---------------------------------------
# GET /workers/health
# ---------------------------------------

@router.get(
    "/workers/health",
    response_model=WorkerHealthListResponse
)
def get_worker_health(
    stale_after_seconds: float = Query(default=15, gt=0, le=3600),
    redis_client=Depends(get_redis)
):
    try:
        metrics_service = WorkerMetrics(redis_client)
        workers = metrics_service.get_all_worker_health(
            stale_after_seconds=stale_after_seconds
        )

        return WorkerHealthListResponse(
            total_workers=len(workers),
            workers=workers
        )

    except redis.RedisError:
        raise HTTPException(
            status_code=503,
            detail="Redis is currently unavailable"
        )

