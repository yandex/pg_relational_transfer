import asyncio
from functools import wraps
from logging import getLogger
import time


logger = getLogger("TIMER")


def timer(func):
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        logger.info("The %s function is completed in %.4f seconds", func.__name__, end_time - start_time)
        return result

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = await func(*args, **kwargs)
        end_time = time.monotonic()
        logger.info("The %s function is completed in %.4f seconds", func.__name__, end_time - start_time)
        return result

    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
