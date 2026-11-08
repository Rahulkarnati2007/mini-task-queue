
import os

import redis


# Redis connection configuration.
# Defaults support local development outside Docker.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))


def get_redis_client():
    """Create and return a Redis client."""

    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )

    return client


def check_redis_connection():
    """Check whether Redis is reachable."""

    client = get_redis_client()

    try:
        response = client.ping()

        if response:
            print(
                f"Successfully connected to Redis "
                f"at {REDIS_HOST}:{REDIS_PORT}!"
            )

        return response

    except redis.RedisError as error:
        print(f"Redis connection failed: {error}")
        return False

    finally:
        client.close()


if __name__ == "__main__":
    check_redis_connection()
