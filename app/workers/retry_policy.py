class RetryPolicy:

    def __init__(
        self,
        base_delay=2,
        max_delay=60
    ):
        if base_delay <= 0:
            raise ValueError("base_delay must be positive")

        if max_delay <= 0:
            raise ValueError("max_delay must be positive")

        self.base_delay = base_delay
        self.max_delay = max_delay

    def calculate_delay(self, attempts: int) -> float:
        if attempts < 1:
            raise ValueError("attempts must be at least 1")

        delay = self.base_delay * (2 ** (attempts - 1))

        return min(delay, self.max_delay)