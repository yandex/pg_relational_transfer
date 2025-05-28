from collections import deque
from typing import Generic, TypeVar


_T = TypeVar("_T")


class NodeQueue(deque[Generic[_T]]):
    def __str__(self):
        return ", ".join(map(str, self))
