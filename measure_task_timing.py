import argparse
import json
import statistics
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

TERMINAL_STATES = {"succeeded", "dead", "cancelled"}


def request_json(url, method="GET", payload=None, timeout=15):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def parse_timestamp(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def percentile(values, percent):
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * percent / 100
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    fraction = position - lower
    return values[lower] * (1 - fraction) + values[upper] * fraction


def print_metric_section(name, values):
    print(f"\n{name}")
    if not values:
        print("  No valid measurements available.")
        return
    print(f"  Samples:  {len(values)}")
    print(f"  Minimum:  {min(values):.4f} seconds")
    print(f"  Average:  {statistics.mean(values):.4f} seconds")
    print(f"  Median:   {statistics.median(values):.4f} seconds")
    print(f"  P95:      {percentile(values, 95):.4f} seconds")
    print(f"  Maximum:  {max(values):.4f} seconds")


def submit_task(base_url, number, task_type):
    payload = {
        "task_type": task_type,
        "payload": {"timing_probe": True, "task_number": number},
        "priority": 5,
        "max_attempts": 1,
    }
    started = time.perf_counter()
    try:
        result = request_json(f"{base_url}/tasks", method="POST", payload=payload)
        return {
            "task_number": number,
            "task_id": result["task_id"],
            "submission_latency_seconds": time.perf_counter() - started,
            "error": None,
        }
    except Exception as error:
        return {
            "task_number": number,
            "task_id": None,
            "submission_latency_seconds": time.perf_counter() - started,
            "error": str(error),
        }


def wait_for_task(base_url, task, timeout):
    task_id = task["task_id"]
    deadline = time.monotonic() + timeout
    polling_errors = 0
    while time.monotonic() < deadline:
        try:
            details = request_json(f"{base_url}/tasks/{task_id}", timeout=15)
            state = details.get("state")
            if state in TERMINAL_STATES:
                return {
                    "task_number": task["task_number"],
                    "task_id": task_id,
                    "state": state,
                    "attempts": details.get("attempts"),
                    "created_at": details.get("created_at"),
                    "started_at": details.get("started_at"),
                    "completed_at": details.get("completed_at"),
                    "polling_errors": polling_errors,
                    "error": None,
                }
        except Exception:
            polling_errors += 1
        time.sleep(0.5)
    return {
        "task_number": task["task_number"],
        "task_id": task_id,
        "state": "timeout",
        "polling_errors": polling_errors,
        "error": "Task did not reach a terminal state in time",
    }


def calculate_timings(task_results):
    queue_wait_times, execution_times, end_to_end_times = [], [], []
    for task in task_results:
        if task.get("state") != "succeeded":
            continue
        created = parse_timestamp(task.get("created_at"))
        started = parse_timestamp(task.get("started_at"))
        completed = parse_timestamp(task.get("completed_at"))
        if not created or not started or not completed:
            continue
        queue_wait = (started - created).total_seconds()
        execution = (completed - started).total_seconds()
        end_to_end = (completed - created).total_seconds()
        if queue_wait < 0 or execution < 0 or end_to_end < 0:
            continue
        task["queue_wait_seconds"] = queue_wait
        task["execution_seconds"] = execution
        task["end_to_end_seconds"] = end_to_end
        queue_wait_times.append(queue_wait)
        execution_times.append(execution)
        end_to_end_times.append(end_to_end)
    return {"queue_wait": queue_wait_times, "execution": execution_times, "end_to_end": end_to_end_times}


def main():
    parser = argparse.ArgumentParser(description="Measure submission, queue waiting, and execution latency.")
    parser.add_argument("--tasks", type=int, default=20)
    parser.add_argument("--task-type", default="process_data")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--submit-workers", type=int, default=10)
    parser.add_argument("--poll-workers", type=int, default=20)
    args = parser.parse_args()

    if min(args.tasks, args.submit_workers, args.poll_workers, args.timeout) < 1:
        parser.error("tasks, workers, and timeout must all be positive")

    print("=" * 68)
    print("MINI TASK QUEUE - TASK TIMING EXPERIMENT")
    print("=" * 68)
    print(f"Tasks requested:    {args.tasks}")
    print(f"Task type:          {args.task_type}")
    print(f"API:                {args.base_url}")
    print(f"Submission workers: {min(args.submit_workers, args.tasks)}")
    print(f"Polling workers:    {min(args.poll_workers, args.tasks)}")

    print("\nSubmitting tasks...")
    submitted_tasks, submission_errors = [], []
    submission_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=min(args.submit_workers, args.tasks)) as executor:
        futures = [executor.submit(submit_task, args.base_url, n, args.task_type) for n in range(1, args.tasks + 1)]
        for future in as_completed(futures):
            result = future.result()
            (submission_errors if result["error"] else submitted_tasks).append(result)
    submission_duration = time.perf_counter() - submission_started

    submission_latencies = [x["submission_latency_seconds"] for x in submitted_tasks + submission_errors]
    print(f"Submitted:             {len(submitted_tasks)}")
    print(f"Submission errors:     {len(submission_errors)}")
    print(f"Total submission time: {submission_duration:.4f} seconds")
    print_metric_section("INDIVIDUAL API SUBMISSION LATENCY", submission_latencies)

    if not submitted_tasks:
        print("No tasks were successfully submitted.")
        return

    print("\nWaiting for tasks to finish...")
    task_results = []
    monitoring_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=min(args.poll_workers, len(submitted_tasks))) as executor:
        futures = [executor.submit(wait_for_task, args.base_url, task, args.timeout) for task in submitted_tasks]
        for future in as_completed(futures):
            task_results.append(future.result())
    monitoring_duration = time.perf_counter() - monitoring_started

    timings = calculate_timings(task_results)
    succeeded = sum(t["state"] == "succeeded" for t in task_results)
    dead = sum(t["state"] == "dead" for t in task_results)
    cancelled = sum(t["state"] == "cancelled" for t in task_results)
    timed_out = sum(t["state"] == "timeout" for t in task_results)
    polling_errors = sum(t.get("polling_errors", 0) for t in task_results)

    print("\n" + "=" * 68)
    print("TASK TIMING RESULTS")
    print("=" * 68)
    print(f"Tasks requested:   {args.tasks}")
    print(f"Tasks submitted:   {len(submitted_tasks)}")
    print(f"Submission errors: {len(submission_errors)}")
    print(f"Tasks succeeded:   {succeeded}")
    print(f"Dead-lettered:     {dead}")
    print(f"Cancelled:         {cancelled}")
    print(f"Timed out:         {timed_out}")
    print(f"Polling errors:    {polling_errors}")
    print_metric_section("QUEUE WAITING TIME", timings["queue_wait"])
    print_metric_section("HANDLER EXECUTION TIME", timings["execution"])
    print_metric_section("END-TO-END TASK TIME", timings["end_to_end"])

    print("\nINDIVIDUAL TASK MEASUREMENTS")
    for task in sorted(task_results, key=lambda item: item["task_number"]):
        if task.get("state") == "succeeded" and "queue_wait_seconds" in task:
            print(f"Task {task['task_number']:02d} | Queue wait: {task['queue_wait_seconds']:.3f}s | Execution: {task['execution_seconds']:.3f}s | Total: {task['end_to_end_seconds']:.3f}s")
    print(f"\nMonitoring duration: {monitoring_duration:.4f} seconds")

    report = {
        "configuration": {
            "tasks_requested": args.tasks,
            "task_type": args.task_type,
            "base_url": args.base_url,
            "submit_workers": min(args.submit_workers, args.tasks),
            "poll_workers": min(args.poll_workers, len(submitted_tasks)),
            "timeout_seconds": args.timeout,
        },
        "summary": {
            "tasks_submitted": len(submitted_tasks),
            "submission_errors": len(submission_errors),
            "succeeded": succeeded,
            "dead": dead,
            "cancelled": cancelled,
            "timed_out": timed_out,
            "polling_errors": polling_errors,
            "submission_duration_seconds": submission_duration,
            "monitoring_duration_seconds": monitoring_duration,
        },
        "submission_timings": {
            "request_latencies_seconds": submission_latencies,
            "average_seconds": statistics.mean(submission_latencies) if submission_latencies else None,
            "median_seconds": statistics.median(submission_latencies) if submission_latencies else None,
            "p95_seconds": percentile(submission_latencies, 95),
            "maximum_seconds": max(submission_latencies) if submission_latencies else None,
        },
        "timings": {
            "queue_wait_seconds": timings["queue_wait"],
            "execution_seconds": timings["execution"],
            "end_to_end_seconds": timings["end_to_end"],
        },
        "submitted_tasks": sorted(submitted_tasks, key=lambda item: item["task_number"]),
        "tasks": sorted(task_results, key=lambda item: item["task_number"]),
        "submission_errors": sorted(submission_errors, key=lambda item: item["task_number"]),
    }
    output_file = "task_timing_results_v2.json"
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)
    print(f"\nDetailed report saved to {output_file}")


if __name__ == "__main__":
    main()
