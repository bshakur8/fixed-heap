from abc import ABC, abstractmethod
from collections import namedtuple
from enum import Enum
from heapq import nlargest, nsmallest, heappush
from random import randint, random
from typing import Any, Callable, List

__all__ = {"HeapFactory"}


class HeapNode(namedtuple("HeapNode", "key value")):
    """A lightweight class that saves data and maintain order"""

    __slots__ = ()

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __repr__(self):
        return f"Key={self.key}: Value={self.value}"


class BaseSorter(ABC):
    @classmethod
    @abstractmethod
    def append(cls, node: HeapNode, heap: "Heap"):
        pass

    @classmethod
    @abstractmethod
    def sort(cls, items, limit):
        pass

    @classmethod
    def index(cls, func, items):
        return func(enumerate(items), key=lambda n: n[1].value)[0]


class Sorter(BaseSorter, ABC):
    @classmethod
    def append(cls, node: HeapNode, heap: "Heap"):
        heap.nodes.append(node)
        if heap.reached_limit():
            cls.handle_limit(heap.nodes)

    @classmethod
    @abstractmethod
    def handle_limit(cls, items: List[HeapNode]):
        pass


class Aggregators:
    @staticmethod
    def sum(obj):
        return sum(obj)

    @staticmethod
    def dummy(o):
        return o


class Heap:
    """Class of all fixed size heap types"""

    def __init__(self, sorter: BaseSorter, limit: int, aggregator: Callable = None):
        self.nodes = []
        self.sorter = sorter
        self.limit = limit
        self.aggregator = aggregator or Aggregators.dummy

    def reached_limit(self):
        return self.limit < len(self)

    def append(self, key: Any, data: Any):
        """Add a new node to heap

        :param key: Node identifier: Data to get after heapify
        :param data: data to run heap on
        """
        node = HeapNode(key=key, value=self.aggregator(data))
        self.sorter.append(node, heap=self)

    def top_n(self):
        for item in self.sorter.sort(self.nodes, self.limit):
            yield item.key

    def __iter__(self):
        yield from self.top_n()

    def __len__(self):
        return len(self.nodes)

    def __repr__(self):
        return f"Heap-{self.sorter.__class__.__name__}: [{len(self.nodes)}/{self.limit}]"


class MinHeap(BaseSorter):
    @classmethod
    def append(cls, node: HeapNode, heap: "Heap"):
        heappush(heap.nodes, node)
        if heap.reached_limit():
            heap.nodes.pop(cls.index(max, heap.nodes))  # O(limit)

    @classmethod
    def sort(cls, items, limit):
        return nsmallest(limit, items)


class MaxHeap(BaseSorter):
    @classmethod
    def append(cls, node: HeapNode, heap: "Heap"):
        heappush(heap.nodes, node)
        if heap.reached_limit():
            heap.nodes.pop(cls.index(min, heap.nodes))  # O(limit)

    @classmethod
    def sort(cls, items, limit):
        return nlargest(limit, items)


class Min(Sorter):
    @classmethod
    def handle_limit(cls, items: List[HeapNode]):
        items.pop(cls.index(max, items))  # O(limit)

    @classmethod
    def sort(cls, items, limit):
        return nsmallest(limit, items)


class Max(Sorter):
    @classmethod
    def handle_limit(cls, items: List[HeapNode]):
        items.pop(cls.index(min, items))  # O(limit)

    @classmethod
    def sort(cls, items, limit):
        return nlargest(limit, items)


class Random(Sorter):
    @classmethod
    def handle_limit(cls, items: List):
        items.pop(randint(0, len(items) - 1))  # nosec

    @classmethod
    def sort(cls, items, *_, **__):
        # return a shuffle copy of heap
        return sorted(items, key=lambda _: random())


class SortType(Enum):
    MIN = Min
    MAX = Max
    MIN_HEAP = MinHeap
    MAX_HEAP = MaxHeap
    RANDOM = Random


class _HeapFactory:
    __slots__ = {}

    @classmethod
    def get(cls, sort_type: str) -> Callable:
        sorter = SortType[sort_type.upper()].value()

        def ctor(limit, aggregator=None) -> Heap:
            return Heap(sorter, limit=limit, aggregator=aggregator)

        return ctor


HeapFactory = _HeapFactory()
