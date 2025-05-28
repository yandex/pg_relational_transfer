import asyncio
from collections.abc import Coroutine


background_tasks = set()


def run_in_background(coroutine: Coroutine, loop: asyncio.AbstractEventLoop):
    task = loop.create_task(coroutine)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
