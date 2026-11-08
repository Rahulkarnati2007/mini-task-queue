
import redis

from fastapi import APIRouter, Depends, HTTPException

from app.api.dependencies import get_redis


router = APIRouter()


@router.get("/health")
def health_check(redis_client=Depends(get_redis)):
    """
    Check whether the API and Redis are working.
    """

    try:

        redis_client.ping()

        return {
            "status": "healthy",
            "application": "mini-task-queue",
            "redis": "connected"
        }

    except redis.RedisError:

        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "redis": "disconnected"
            }
        )
        
