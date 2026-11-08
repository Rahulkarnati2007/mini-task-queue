"""
Mini Task Queue - load test v3.

Improvements over v2:
- Reuses persistent HTTP connections per thread (HTTP keep-alive).
- Retries one time after transient socket/connection errors.
- Keeps polling staggered with exponential backoff.
- Reports submission, completion, and polling metrics separately.

Run FastAPI and your worker processes before executing this script.
Uses only the Python standard library.
"""

import argparse
import http.client
import json
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlsplit


TERMINAL_STATES = {"succeeded", "dead", "cancelled"}
_THREAD_LOCAL = threading.local()


class PersistentHttpClient:
    """One reusable HTTP connection per thread."""

    def __init__(self, base_url, timeout=15):
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("Base URL must use http or https")

        self.host = parsed.hostname
        self.port = parsed.port
        self.timeout = timeout
        self.use_tls = parsed.scheme == "https"
        self.base_path = parsed.path.rstrip("/")
        self.connection = None

    def _new_connection(self):
        connection_type = (
            http.client.HTTPSConnection
            if self.use_tls
            else http.client.HTTPConnection
        )
        self.connection = connection_type(
            self.host,
            self.port,
            timeout=self.timeout,
        )

    def request_json(self, path, method="GET", payload=None):
        if self.connection is None:
            self._new_connection()

        body = None
        headers = {"Connection": "keep-alive"}

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        full_path = f"{self.base_path}{path}"

        # A stale keep-alive connection can be closed by the server.
        # Retry once using a fresh connection.
        for attempt in range(2):
            try:
                self.connection.request(
                    method,
                    full_path,
                    body=body,
                    headers=headers,
                )
                response = self.connection.getresponse()
                raw_body = response.read()
                status = response.status

                if status < 200 or status >= 300:
                    message = raw_body.decode("utf-8", errors="replace")
                    raise http.client.HTTPException(
                        f"HTTP {status}: {message[:300]}"
                    )

                if not raw_body:
                    return {}

                return json.loads(raw_body.decode("utf-8"))

            except (OSError, http.client.HTTPException, TimeoutError):
                self.close()
                if attempt == 1:
                    raise
                self._new_connection()

        raise RuntimeError("HTTP request failed unexpectedly")

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except OSError:
                pass
            self.connection = None


def get_client(base_url):
    client = getattr(_THREAD_LOCAL, "client", None)
    if client is None:
        client = PersistentHttpClient(base_url)
        _THREAD_LOCAL.client = client
    return client


def percentile(values, percentile_value):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile_value / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def submit_one_task(base_url, task_number, task_type, priority, max_attempts):
    payload = {
        "task_type": task_type,
        "payload": {
            "load_test": True,
            "task_number": task_number,
            "report_name": f"load_test_report_{task_number}",
            "to": f"load-test-{task_number}@example.com",
        },
        "priority": priority,
        "max_attempts": max_attempts,
    }

    started = time.perf_counter()
    try:
        result = get_client(base_url).request_json(
            "/tasks", method="POST", payload=payload
        )
        finished = time.perf_counter()
        return {
            "task_number": task_number,
            "task_id": result["task_id"],
            "submit_latency": finished - started,
            "submitted_at": finished,
            "error": None,
        }
    except Exception as error:
        finished = time.perf_counter()
        return {
            "task_number": task_number,
            "task_id": None,
            "submit_latency": finished - started,
            "submitted_at": finished,
            "error": f"{type(error).__name__}: {error}",
        }


def poll_one_task(base_url, task):
    started = time.perf_counter()
    try:
        result = get_client(base_url).request_json(
            f"/tasks/{task['task_id']}"
        )
        return {
            "task_id": task["task_id"],
            "state": result.get("state"),
            "error": None,
            "request_latency": time.perf_counter() - started,
        }
    except Exception as error:
        return {
            "task_id": task["task_id"],
            "state": None,
            "error": f"{type(error).__name__}: {error}",
            "request_latency": time.perf_counter() - started,
        }


def wait_for_completion(
    base_url,
    submitted_tasks,
    poll_workers,
    initial_poll_interval,
    max_poll_interval,
    timeout,
):
    started = time.perf_counter()
    pending = {}

    for task in submitted_tasks:
        task["poll_interval"] = initial_poll_interval
        task["next_poll_at"] = started
        pending[task["task_id"]] = task

    completed = []
    polling_errors = 0
    error_types = Counter()
    error_examples = []
    poll_requests = 0
    poll_latency_total = 0.0

    with ThreadPoolExecutor(max_workers=poll_workers) as executor:
        while pending:
            elapsed = time.perf_counter() - started
            if elapsed >= timeout:
                break

            now = time.perf_counter()
            due = [
                task for task in pending.values()
                if task["next_poll_at"] <= now
            ]

            if not due:
                next_due = min(
                    task["next_poll_at"] for task in pending.values()
                )
                time.sleep(min(max(next_due - time.perf_counter(), 0.05), 0.5))
                continue

            futures = {
                executor.submit(poll_one_task, base_url, task): task
                for task in due
            }

            for future in as_completed(futures):
                task = futures[future]
                result = future.result()
                poll_requests += 1
                poll_latency_total += result["request_latency"]

                if result["error"]:
                    polling_errors += 1
                    error_name = result["error"].split(":", 1)[0]
                    error_types[error_name] += 1
                    if len(error_examples) < 10:
                        error_examples.append({
                            "task_id": task["task_id"],
                            "error": result["error"][:400],
                        })

                elif result["state"] in TERMINAL_STATES:
                    task["final_state"] = result["state"]
                    # Observation time includes the polling interval.
                    task["observed_completion_latency"] = (
                        time.perf_counter() - task["submitted_at"]
                    )
                    completed.append(task)
                    pending.pop(task["task_id"], None)
                    continue

                task["next_poll_at"] = (
                    time.perf_counter() + task["poll_interval"]
                )
                task["poll_interval"] = min(
                    task["poll_interval"] * 2,
                    max_poll_interval,
                )

    details = {
        "poll_requests": poll_requests,
        "polling_errors": polling_errors,
        "polling_error_types": dict(error_types),
        "polling_error_examples": error_examples,
        "average_poll_request_latency_seconds": (
            poll_latency_total / poll_requests if poll_requests else None
        ),
    }
    return completed, list(pending.values()), details


def main():
    parser = argparse.ArgumentParser(
        description="Load test the Mini Task Queue using persistent HTTP connections."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--tasks", type=int, default=1000)
    parser.add_argument("--submit-workers", type=int, default=20)
    parser.add_argument("--poll-workers", type=int, default=10)
    parser.add_argument("--initial-poll-interval", type=float, default=0.5)
    parser.add_argument("--max-poll-interval", type=float, default=3.0)
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument(
        "--task-type",
        choices=["process_data", "send_email", "generate_report"],
        default="process_data",
    )
    parser.add_argument("--priority", type=int, default=5)
    parser.add_argument("--max-attempts", type=int, default=1)
    args = parser.parse_args()

    if args.tasks < 1:
        parser.error("--tasks must be at least 1")
    if args.submit_workers < 1 or args.poll_workers < 1:
        parser.error("worker counts must be at least 1")
    if args.initial_poll_interval <= 0 or args.max_poll_interval <= 0:
        parser.error("poll intervals must be positive")
    if args.max_poll_interval < args.initial_poll_interval:
        parser.error("max poll interval must be >= initial poll interval")
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    if not 0 <= args.priority <= 9:
        parser.error("priority must be between 0 and 9")
    if not 1 <= args.max_attempts <= 10:
        parser.error("max attempts must be between 1 and 10")

    base_url = args.base_url.rstrip("/")

    try:
        get_client(base_url).request_json("/stats")
    except Exception as error:
        raise SystemExit(
            f"Cannot reach API at {base_url}. "
            f"Start FastAPI and verify Redis. Error: {error}"
        )

    print(f"Submitting {args.tasks} tasks of type '{args.task_type}'...")
    benchmark_started = time.perf_counter()
    submitted = []
    submission_errors = []

    with ThreadPoolExecutor(max_workers=args.submit_workers) as executor:
        futures = [
            executor.submit(
                submit_one_task,
                base_url,
                number,
                args.task_type,
                args.priority,
                args.max_attempts,
            )
            for number in range(1, args.tasks + 1)
        ]
        for future in as_completed(futures):
            result = future.result()
            if result["error"]:
                submission_errors.append(result)
            else:
                submitted.append(result)

    submission_finished = time.perf_counter()
    submission_duration = submission_finished - benchmark_started
    print(
        f"Submitted {len(submitted)}/{args.tasks} tasks "
        f"in {submission_duration:.2f} seconds."
    )

    if submitted:
        print("Monitoring task states with persistent connections...")
        completed, timed_out, polling = wait_for_completion(
            base_url,
            submitted,
            args.poll_workers,
            args.initial_poll_interval,
            args.max_poll_interval,
            args.timeout,
        )
    else:
        completed, timed_out = [], []
        polling = {
            "poll_requests": 0,
            "polling_errors": 0,
            "polling_error_types": {},
            "polling_error_examples": [],
            "average_poll_request_latency_seconds": None,
        }

    total_duration = time.perf_counter() - benchmark_started
    succeeded = sum(t["final_state"] == "succeeded" for t in completed)
    dead = sum(t["final_state"] == "dead" for t in completed)
    cancelled = sum(t["final_state"] == "cancelled" for t in completed)

    submit_latencies = [t["submit_latency"] for t in submitted]
    completion_latencies = [
        t["observed_completion_latency"] for t in completed
    ]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tasks_requested": args.tasks,
        "tasks_submitted": len(submitted),
        "submission_errors": len(submission_errors),
        "tasks_completed": len(completed),
        "succeeded": succeeded,
        "dead": dead,
        "cancelled": cancelled,
        "timed_out_or_still_active": len(timed_out),
        "submission_duration_seconds": round(submission_duration, 4),
        "total_duration_seconds": round(total_duration, 4),
        "submission_throughput_tasks_per_second": round(
            len(submitted) / submission_duration, 4
        ) if submission_duration else 0,
        "end_to_end_completion_throughput_tasks_per_second": round(
            len(completed) / total_duration, 4
        ) if total_duration else 0,
        "average_submission_latency_seconds": round(
            statistics.mean(submit_latencies), 4
        ) if submit_latencies else None,
        "p95_submission_latency_seconds": round(
            percentile(submit_latencies, 95), 4
        ) if submit_latencies else None,
        "average_observed_completion_latency_seconds": round(
            statistics.mean(completion_latencies), 4
        ) if completion_latencies else None,
        "p50_observed_completion_latency_seconds": round(
            percentile(completion_latencies, 50), 4
        ) if completion_latencies else None,
        "p95_observed_completion_latency_seconds": round(
            percentile(completion_latencies, 95), 4
        ) if completion_latencies else None,
        "p99_observed_completion_latency_seconds": round(
            percentile(completion_latencies, 99), 4
        ) if completion_latencies else None,
        "success_rate_percent": round(
            succeeded / len(submitted) * 100, 2
        ) if submitted else None,
        **polling,
        "submission_error_examples": submission_errors[:10],
        "timed_out_task_ids": [t["task_id"] for t in timed_out[:20]],
    }

    print("\n" + "=" * 68)
    print("MINI TASK QUEUE - LOAD TEST V3 RESULTS")
    print("=" * 68)
    print(f"Tasks requested:                       {args.tasks}")
    print(f"Tasks submitted:                       {len(submitted)}")
    print(f"Submission errors:                     {len(submission_errors)}")
    print(f"Tasks reaching terminal state:         {len(completed)}")
    print(f"  Succeeded:                           {succeeded}")
    print(f"  Dead-lettered:                       {dead}")
    print(f"  Cancelled:                           {cancelled}")
    print(f"Timed out / still active:              {len(timed_out)}")
    print("-" * 68)
    print(f"Submission duration:                  {submission_duration:.2f} s")
    print(f"Total duration:                       {total_duration:.2f} s")
    print(f"Submission throughput:                {report['submission_throughput_tasks_per_second']:.2f} tasks/s")
    print(f"Completion throughput:                {report['end_to_end_completion_throughput_tasks_per_second']:.2f} tasks/s")
    print(f"Average submission latency:           {report['average_submission_latency_seconds']:.4f} s")
    print(f"P95 submission latency:                {report['p95_submission_latency_seconds']:.4f} s")

    for label, key in [
        ("Average observed completion latency", "average_observed_completion_latency_seconds"),
        ("P50 observed completion latency", "p50_observed_completion_latency_seconds"),
        ("P95 observed completion latency", "p95_observed_completion_latency_seconds"),
        ("P99 observed completion latency", "p99_observed_completion_latency_seconds"),
    ]:
        value = report[key]
        if value is not None:
            print(f"{label}: {value:.4f} s")

    print("-" * 68)
    print(f"Polling requests:                     {polling['poll_requests']}")
    print(f"Polling errors:                       {polling['polling_errors']}")
    print(f"Average poll request latency:         {polling['average_poll_request_latency_seconds'] or 0:.4f} s")
    if polling["polling_error_types"]:
        print("Polling error breakdown:")
        for name, count in sorted(polling["polling_error_types"].items()):
            print(f"  {name}: {count}")
    if polling["polling_error_examples"]:
        print("First polling error examples:")
        for item in polling["polling_error_examples"][:5]:
            print(f"  {item['task_id']}: {item['error']}")
    print("=" * 68)

    with open("load_test_results_v3.json", "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    print("\nDetailed report saved to: load_test_results_v3.json")

    # Close the main thread's health-check connection.
    get_client(base_url).close()


if __name__ == "__main__":
    main()
