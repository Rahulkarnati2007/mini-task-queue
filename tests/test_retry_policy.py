import pytest

from app.workers.retry_policy import RetryPolicy


def test_first_attempt_delay():
    policy = RetryPolicy(base_delay=2, max_delay=60)
    assert policy.calculate_delay(1) == 2


def test_second_attempt_delay():
    policy = RetryPolicy(base_delay=2, max_delay=60)
    assert policy.calculate_delay(2) == 4


def test_third_attempt_delay():
    policy = RetryPolicy(base_delay=2, max_delay=60)
    assert policy.calculate_delay(3) == 8


def test_delay_is_capped():
    policy = RetryPolicy(base_delay=2, max_delay=60)
    assert policy.calculate_delay(10) == 60


def test_invalid_attempt_count():
    policy = RetryPolicy()

    with pytest.raises(ValueError):
        policy.calculate_delay(0)


def test_invalid_base_delay():
    with pytest.raises(ValueError):
        RetryPolicy(base_delay=0)


def test_invalid_max_delay():
    with pytest.raises(ValueError):
        RetryPolicy(max_delay=0)