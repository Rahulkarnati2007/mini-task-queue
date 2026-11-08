
import time


def send_email(payload: dict):
    """
    Simulates sending an email.
    """

    recipient = payload.get("to")
    subject = payload.get("subject")

    print(f"Sending email to: {recipient}")
    print(f"Subject: {subject}")

    # Simulate some processing time
    time.sleep(2)

    print("Email task completed successfully!")

    return {
        "status": "success",
        "recipient": recipient
    }


def generate_report(payload: dict):
    """
    Simulates generating a report.
    """

    report_name = payload.get(
        "report_name",
        "default_report"
    )

    print(f"Generating report: {report_name}")

    time.sleep(3)

    print("Report generated successfully!")

    return {
        "status": "success",
        "report_name": report_name
    }


def process_data(payload: dict):
    """
    Simulates processing data.
    """

    print("Processing data...")

    time.sleep(1)

    print("Data processing completed!")

    return {
        "status": "success"
    }


# Registry connecting task types to functions
TASK_HANDLERS = {
    "send_email": send_email,
    "generate_report": generate_report,
    "process_data": process_data
}


def execute_handler(task_type: str, payload: dict):
    """
    Finds and executes the correct handler.
    """

    handler = TASK_HANDLERS.get(task_type)

    if handler is None:
        raise ValueError(
            f"No handler registered for task type: {task_type}"
        )

    return handler(payload)
