import operator
from abc import ABC
from collections import namedtuple
from enum import Enum
from heapq import heappush, heapify, heappop, nlargest, heappushpop
from random import randint, shuffle
from typing import Any, Dict, Callable

__all__ = {'HeapFactory'}


def sum_aggregator(obj):
    return sum(obj)


class Heap(ABC):
    """Abstract class of all fixed size heap types"""

    def __init__(self, limit: int, aggregator: Callable = None):
        self.heap = []
        self.limit = limit
        self.aggregator = aggregator or sum_aggregator

    def append(self, key: Any, data: Dict):
        """Add a new node to heap

        :param key: Node identifier: Data to get after heapify
        :param data: data to run heap on
        """
        self._add_node(HeapNode(key=key, value=self.aggregator(data)))

    def _add_node(self, node):
        if len(self.heap) < self.limit:
            heappush(self.heap, node)
        else:
            self._handle_limit(node)

    def _handle_limit(self, node):
        raise NotImplementedError()

    def top_n(self):
        raise NotImplementedError()

    def __iter__(self):
        yield from self.top_n()

    def __len__(self):
        return len(self.heap)

    def __repr__(self):
        return f"{self.__class__.__name__}: [{len(self.heap)}/{self.limit}]"


class HeapNode(namedtuple('Node', 'key value')):
    """A lightweight heap node: saves data and maintain order"""

    __slots__ = ()

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __repr__(self):
        return f"{self.key}: {self.value}"


class MinHeap(Heap):
    def _handle_limit(self, node):
        heappush(self.heap, node)
        max_index = max(enumerate(self.heap), key=operator.itemgetter(1))[0]  # O(n)
        self.heap.pop(max_index)

    def top_n(self):
        heapify(self.heap)
        while len(self.heap) > 0:
            yield heappop(self.heap).key


class MaxHeap(Heap):
    def _handle_limit(self, node: HeapNode):
        heappushpop(self.heap, node)

    def top_n(self):
        self.heap = nlargest(self.limit, self.heap)
        while len(self.heap) > 0:
            yield self.heap.pop(0).key


class RandomHeap(Heap):
    def _add_node(self, node):
        self.heap.append(node)
        if len(self.heap) > self.limit:
            self._handle_limit(node)

    def _handle_limit(self, node: HeapNode):
        self.heap.pop(randint(0, len(self.heap) - 1))  # nosec

    def top_n(self):
        shuffle(self.heap)
        while len(self.heap) > 0:
            yield self.heap.pop(0).key


class HeapType(Enum):
    MIN = "min"
    MAX = "max"
    RANDOM = "random"


class _HeapFactory:
    __slots__ = {}

    _HEAPS_INDEX = {
        HeapType.MIN: MinHeap,
        HeapType.MAX: MaxHeap,
        HeapType.RANDOM: RandomHeap,
    }

    @classmethod
    def max(cls, limit, aggregator=None):
        return MaxHeap(limit, aggregator=aggregator)

    @classmethod
    def min(cls, limit, aggregator=None):
        return MinHeap(limit, aggregator=aggregator)

    @classmethod
    def random(cls, limit, aggregator=None):
        return RandomHeap(limit, aggregator=aggregator)

    @classmethod
    def get(cls, heap_type: str) -> Callable:
        heap = cls._HEAPS_INDEX.get(HeapType(heap_type.lower()), MaxHeap)

        def ctor(limit, aggregator=None) -> Heap:
            return heap(limit, aggregator=aggregator)

        return ctor


HeapFactory = _HeapFactory()
