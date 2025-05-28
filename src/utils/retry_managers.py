import asyncio
from contextlib import asynccontextmanager, contextmanager
from logging import getLogger
import time


logger = getLogger("RETRY_MANAGERS")


@contextmanager
def retry_sync(exceptions: type[BaseException] | tuple[type[BaseException], ...], retries=3, delay=1):
    attempt = 0
    while attempt <= retries:
        try:
            yield
            break
        except exceptions:
            attempt += 1
            if attempt > retries:
                logger.error("All attempts have been exhausted")
                raise
            logger.warning("Attempt %d failed, retrying in %d seconds...", attempt + 1, delay)
            time.sleep(delay)


@asynccontextmanager
async def retry_async(exceptions: type[BaseException] | tuple[type[BaseException], ...], retries=3, delay=1):
    attempt = 0
    while attempt <= retries:
        try:
            yield
            break
        except exceptions:
            attempt += 1
            if attempt > retries:
                logger.error("All attempts have been exhausted")
                raise
            logger.warning("Attempt %d failed, retrying in %d seconds...", attempt + 1, delay)
            await asyncio.sleep(delay)
