# Mini Task Queue

A distributed background task queue built using **Python, FastAPI, and Redis**, supporting multiple workers, task prioritization, automatic retries, crash recovery, and task monitoring.

This project demonstrates important distributed-systems concepts such as concurrency, atomic operations, fault tolerance, lease management, and at-least-once task delivery.

## Features

- **Multi-worker execution:** Multiple workers process tasks concurrently.
- **Priority scheduling:** Tasks are processed according to priority.
- **Atomic task reservation:** Redis Lua scripts prevent concurrent workers from reserving the same task.
- **Automatic retries:** Exponential backoff with jitter and configurable attempt limits.
- **Delayed execution:** Schedule tasks for future execution.
- **Dead Letter Queue (DLQ):** Isolate tasks that exhaust their retry attempts.
- **Crash recovery:** Automatically recover tasks from workers that crash.
- **Lease management:** Heartbeats renew active leases.
- **Stale-worker protection:** Lease tokens prevent outdated workers from acknowledging reassigned tasks.
- **Graceful shutdown:** Workers handle shutdown signals safely.
- **Idempotency support:** Prevent duplicate task submissions using idempotency keys.
- **Monitoring:** Queue statistics, worker metrics, and worker health endpoints.
- **Docker deployment:** Run the complete system using Docker Compose.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.12 | Core programming language |
| FastAPI | REST API |
| Redis 7 | Queue coordination and task storage |
| Redis Lua | Atomic queue operations |
| Docker | Containerization |
| Docker Compose | Multi-service deployment |
| Pytest | Automated testing |

## Architecture

```text
                 Client
                   |
                   v
              FastAPI API
                   |
                   v
                 Redis
                   |
              Ready Queue
                   |
          +--------+--------+
          |        |        |
          v        v        v
       Worker 1 Worker 2 Worker 3 ... Worker 4
          |        |        |
          +--------+--------+
                   |
             Task Execution
                   |
             +-----+-----+
             |           |
             v           v
          Success      Failure
                         |
                    Retry / DLQ

Background Services:
- Delayed Scheduler
- Recovery Worker
```

## Project Structure

```text
mini-task-queue/
│
├── app/
│   ├── api/
│   ├── core/
│   ├── storage/
│   └── workers/
│
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .dockerignore
├── README.md
└── load_test_v3.py
```

## Getting Started

### Prerequisites

- Python 3.12+
- Docker Desktop
- Docker Compose

### 1. Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd mini-task-queue
```

### 2. Start the application

```bash
docker compose up --build --scale worker=4 -d
```

This starts:

- Redis
- FastAPI application
- Four worker instances
- Delayed scheduler
- Recovery worker

### 3. Verify services

```bash
docker compose ps
```

View application logs:

```bash
docker compose logs --tail=50
```

### 4. Access the application

| Service | URL |
|---|---|
| Swagger API Documentation | http://localhost:8000/docs |
| Health Check | http://localhost:8000/health |

### 5. Stop the application

```bash
docker compose down
```

Redis data is persisted using a Docker volume.

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/tasks` | Submit a task |
| GET | `/tasks` | List tasks |
| GET | `/tasks/{task_id}` | Retrieve task details |
| DELETE | `/tasks/{task_id}` | Cancel a task |
| GET | `/stats` | Queue statistics |
| GET | `/dlq` | View dead-letter tasks |
| POST | `/dlq/{task_id}/requeue` | Requeue a dead-letter task |
| GET | `/workers/metrics` | Worker performance metrics |
| GET | `/workers/health` | Worker health information |
| GET | `/health` | API and Redis health check |

## Example Task Submission

Submit a task using `POST /tasks`:

```json
{
  "task_type": "process_data",
  "payload": {
    "data": [10, 20, 30, 40, 50],
    "operation": "sum"
  },
  "max_attempts": 3
}
```

A successful submission returns HTTP `201` and a unique task ID.

Retrieve task details using:

```text
GET /tasks/{task_id}
```

A successful task follows this lifecycle:

```text
PENDING → RUNNING → SUCCEEDED
```

## Reliability and Fault Tolerance

### 1. Atomic Task Reservation
Redis Lua scripts ensure that task reservation and queue transitions execute atomically.

### 2. Lease-Based Processing
Workers receive leases while processing tasks.

### 3. Heartbeat Mechanism
Workers periodically renew their leases to indicate that they are still active.

### 4. Crash Recovery
The recovery worker detects expired leases and recovers unfinished tasks.

### 5. Retry Mechanism
Failed tasks are retried using exponential backoff and jitter, subject to configurable attempt limits.

### 6. Dead Letter Queue
Tasks that exhaust their allowed attempts are moved to the DLQ.

### 7. Stale Worker Protection
Lease tokens prevent workers with outdated reservations from acknowledging tasks.

**Delivery semantics:** The system provides at-least-once execution. A task may execute more than once under certain failure conditions, so task handlers should be idempotent where necessary.

## Testing

Run the automated test suite:

```bash
python -m pytest -v
```

**Development testing:** All 55 tests passed during development.

## Load Testing

A load test was conducted using 1,000 `process_data` tasks and four workers.

### Results

| Metric | Result |
|---|---:|
| Tasks submitted | 1,000 |
| Tasks succeeded | 1,000 |
| Failed / dead / timed out | 0 |
| Submission errors | 0 |
| Polling errors | 0 |
| Submission throughput | 57.46 tasks/sec |
| Completion throughput | 3.59 tasks/sec |
| Average submission latency | 0.335 sec |
| P95 submission latency | 0.682 sec |

*Results were obtained in a local development environment. Performance may vary depending on hardware and workload.*

## Design Decisions

- Redis provides fast queue operations and atomic Lua-script execution.
- Multiple workers enable concurrent task processing.
- Lease-based recovery helps handle worker crashes.
- Retry limits prevent infinite retry loops.
- The DLQ isolates repeatedly failing tasks.
- At-least-once delivery provides recovery from execution failures without claiming exactly-once processing.

## Future Improvements

- Authentication and authorization.
- Distributed tracing and structured logging.
- Rate limiting.
- Advanced monitoring dashboard.
- Additional task handlers.
- Production deployment and horizontal scaling.

## Author

GitHub: <YOUR_GITHUB_PROFILE_URL>

