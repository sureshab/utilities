import logging
from functools import wraps
from random import uniform
from time import sleep
from typing import Any
from typing import Tuple
from typing import Union


BACKOFF_FACTOR = 4.5
MAX_BACKOFF = 20
MAX_RETRY = 3

logger = logging.getLogger(__name__)


def retry(
    exceptions: Union[Tuple, Exception] = Exception,
    backoff_factor: float = MAX_BACKOFF,
    max_retry: int = MAX_RETRY,
    max_backoff: int = MAX_BACKOFF,
) -> Any:
    """
    attempts to implement simple retry decorator with "Full Jitter"
    algorithm as described in below link
    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    :param exceptions: Exception or tuple of Exception to retry on
    :param backoff_factor: backoff factor multiplier for each retry
    :param max_retry: max retry count
    :param max_backoff: max sleep time between requests

    Example Usage:
    from retry_decorator import retry

    @retry()
    def test(*args, **kwargs):
        <business_logic>
        return

    @retry(exceptions=ZeroDivisionError, max_retry=10)
    def test(*args, **kwargs):
        <business_logic>
        return

    @retry(exceptions=(AttributeError, ZeroDivisionError), backoff_factor=1.3, max_backoff=3, max_retry=3)
    def test(*args, **kwargs):
        <business_logic>
        return

    """
    # sanity check
    if not isinstance(backoff_factor, float):
        backoff_factor = MAX_BACKOFF
    if not isinstance(max_backoff, int):
        max_backoff = MAX_BACKOFF
    if not isinstance(max_retry, int):
        max_retry = MAX_RETRY

    def inner_func(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            request_count = 1
            while request_count < max_retry:
                try:
                    return func(*args, **kwargs)
                # Exception or Tuple of Exception
                except exceptions as e:
                    delay = min(max_backoff, backoff_factor * 2 ** request_count)
                    delay = (delay / 2) + uniform(0, delay / 2)

                    logger.info(
                        f"Caught exception : {e.__str__()}. Retrying in {delay} seconds"
                    )
                    print(
                        f"Caught exception : {e.__str__()}. Retrying in {delay} seconds"
                    )
                    sleep(delay)
                    request_count += 1
            return func(*args, **kwargs)

        return wrapper

    return inner_func
