
from app.storage.redis_client import get_redis_client


def main():

    redis_client = get_redis_client()

    # 1. Test connection
    print("PING:", redis_client.ping())

    # 2. Test strings
    redis_client.set("project_name", "Mini Task Queue")

    print(
        "Project:",
        redis_client.get("project_name")
    )

    # 3. Test hashes
    redis_client.hset(
        "task:101",
        mapping={
            "type": "send_email",
            "state": "pending",
            "attempts": 0
        }
    )

    print(
        "Task information:",
        redis_client.hgetall("task:101")
    )

    # 4. Test sorted sets
    redis_client.zadd(
        "tasks:ready",
        {
            "task101": 10,
            "task102": 20,
            "task103": 5
        }
    )

    print(
        "Ready queue:",
        redis_client.zrange(
            "tasks:ready",
            0,
            -1,
            withscores=True
        )
    )

    print("\nAll Redis operations completed!")


if __name__ == "__main__":
    main()
