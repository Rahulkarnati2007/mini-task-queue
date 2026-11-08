
from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.tasks import router as tasks_router


# Create FastAPI application
app = FastAPI(
    title="Mini Task Queue",
    description=(
        "A multi-worker background task queue "
        "built using Python, FastAPI and Redis."
    ),
    version="1.0.0"
)


# Register routers
app.include_router(health_router)
app.include_router(tasks_router)


@app.get("/")
def root():

    return {
        "message": "Welcome to Mini Task Queue",
        "documentation": "/docs",
        "health_check": "/health"
    }
