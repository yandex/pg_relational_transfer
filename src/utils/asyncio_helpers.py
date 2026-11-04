import asyncio
from collections.abc import Coroutine
from logging import getLogger


background_tasks = set()
logger = getLogger("ASYNCIO_HELPERS")


async def fire_and_forget_safely(coroutine: Coroutine):
    try:
        await coroutine
    except Exception:
        logger.exception("Exception occurred in background task")


def run_in_background(coroutine: Coroutine):
    task = asyncio.create_task(fire_and_forget_safely(coroutine))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
