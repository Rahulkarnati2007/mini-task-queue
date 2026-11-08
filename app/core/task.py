
# from datetime import datetime, timezone
# from enum import Enum
# from typing import Any
# from uuid import uuid4

# from pydantic import BaseModel, Field


# # 1. Define all possible task states
# class TaskState(str, Enum):

#     PENDING = "pending"
#     RUNNING = "running"
#     SUCCEEDED = "succeeded"
#     FAILED = "failed"
#     DEAD = "dead"
#     DELAYED = "delayed"
#     CANCELLED = "cancelled"


# # 2. Helper function for UTC timestamps
# def current_timestamp() -> datetime:

#     return datetime.now(timezone.utc)


# # 3. Model for incoming task requests
# class TaskCreate(BaseModel):

#     task_type: str = Field(
#         min_length=1,
#         max_length=100
#     )

#     payload: dict[str, Any] = Field(
#         default_factory=dict
#     )

#     priority: int = Field(
#         default=5,
#         ge=0,
#         le=9
#     )

#     max_attempts: int = Field(
#         default=3,
#         ge=1,
#         le=10
#     )

#     idempotency_key: str | None = Field(
#         default=None,
#         max_length=255
#     )


# # 4. Complete task data model
# class Task(BaseModel):

#     id: str = Field(
#         default_factory=lambda: str(uuid4())
#     )

#     task_type: str

#     payload: dict[str, Any] = Field(
#         default_factory=dict
#     )

#     priority: int = Field(
#         default=5,
#         ge=0,
#         le=9
#     )

#     state: TaskState = TaskState.PENDING

#     attempts: int = 0

#     max_attempts: int = 3

#     idempotency_key: str | None = None

#     created_at: datetime = Field(
#         default_factory=current_timestamp
#     )

#     updated_at: datetime = Field(
#         default_factory=current_timestamp
#     )

#     started_at: datetime | None = None

#     completed_at: datetime | None = None

#     last_error: str | None = None


# # 5. Convert incoming request into complete task
# def create_task(request: TaskCreate) -> Task:

#     return Task(
#         task_type=request.task_type,
#         payload=request.payload,
#         priority=request.priority,
#         max_attempts=request.max_attempts,
#         idempotency_key=request.idempotency_key
#     )

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD = "dead"
    DELAYED = "delayed"
    CANCELLED = "cancelled"


def current_timestamp() -> datetime:
    return datetime.now(timezone.utc)


class TaskCreate(BaseModel):
    task_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=0, le=9)
    max_attempts: int = Field(default=3, ge=1, le=10)
    idempotency_key: str | None = Field(
        default=None,
        max_length=255
    )


class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)

    priority: int = Field(default=5, ge=0, le=9)
    state: TaskState = TaskState.PENDING

    attempts: int = 0
    max_attempts: int = 3

    idempotency_key: str | None = None

    created_at: datetime = Field(default_factory=current_timestamp)
    updated_at: datetime = Field(default_factory=current_timestamp)

    started_at: datetime | None = None
    completed_at: datetime | None = None
    retry_at: datetime | None = None

    last_error: str | None = None


def create_task(request: TaskCreate) -> Task:
    return Task(
        task_type=request.task_type,
        payload=request.payload,
        priority=request.priority,
        max_attempts=request.max_attempts,
        idempotency_key=request.idempotency_key
    )