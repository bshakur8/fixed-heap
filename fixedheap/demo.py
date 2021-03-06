import operator
import sys
from itertools import product
from pprint import pprint
from random import shuffle, randint
from typing import Optional

import better_exceptions

from fixedheap.heap import HeapFactory

better_exceptions.hook()


def type_aggregator(parts, aggr_func=sum):
    def wrap(data):
        return aggr_func(sum(data[a]) for a in parts)

    return wrap


def get_random_list(random_range, data_size):
    data = [randint(*random_range) for i in range(data_size)]
    shuffle(data)
    return data


def get_random_data(
    size=10,
    data_size=10,
    random_range=(1, 100),
    data_type=Optional[type],
    keys=None,
    nested_dict=False,
):
    items = {}
    for i in range(size):
        if data_type is dict:
            assert keys
            if nested_dict:
                items[i] = {c: get_random_list(random_range, data_size) for c in keys}
            else:
                for c in keys:
                    items[(i, c)] = get_random_list(random_range, data_size)
        else:
            items[i] = get_random_list(random_range, data_size)
    return items


def simple_usage(heap_type="max"):
    limit = 3
    items = get_random_data(size=10, random_range=(0, 20))

    for item in items.values():
        pprint(f"{item}, {sum(item)}")

    # Take a max heap with limit 3
    heap = HeapFactory.get(heap_type)(limit=limit, aggregator=lambda x: sum(x))

    # put data in heap; mark keys as items to get after heap
    for key, data in items.items():
        heap.append(key=(key, data), data=data)

    check_heap(heap, heap_type, limit=limit)


def advanced_usage(heap_type="min"):
    # start a max heap with limit 2 and set average aggregator
    limit = 10
    items = get_random_data(
        size=10, data_type=dict, keys=["east", "west", "north", "south"]
    )

    heap = HeapFactory.get(heap_type)(limit=limit, aggregator=lambda x: sum(x) / len(x))

    # put data in heap; mark keys as items to get after heap
    for key, data in items.items():
        heap.append(key=(key, data), data=data)

    check_heap(heap, heap_type, limit=limit)


def check_heap(heap, heap_type, limit):
    start_value, operate = (
        (sys.maxsize, operator.le)
        if heap_type.startswith("max")
        else (-sys.maxsize, operator.ge)
    )
    assert len(heap) == limit, len(heap)
    # iterate over heap to get sorted items
    for key, data in heap:
        agg = heap.aggregator(data)
        print(f"{key=}, {agg=}")
        assert isinstance(agg, (float, int)), type(agg)
        assert operate(agg, start_value)
        start_value = agg


def advanced_usage_2(heap_type="max"):
    limit = 2
    # start a max heap with limit 2 and set average aggregator
    keys = ["east", "west", "north", "south"]
    items = get_random_data(data_type=dict, keys=keys, nested_dict=True)

    heap = HeapFactory.get(heap_type)(
        limit=limit, aggregator=type_aggregator(parts=["east", "south"])
    )

    # put data in heap; mark keys as items to get after heap
    for key, data in items.items():
        heap.append(key=(key, data), data=data)

    check_heap(heap, heap_type, limit=limit)


def simple():
    simple_usage("max_heap")


def all_cases():
    for case, _type in product(
        (simple_usage, advanced_usage, advanced_usage_2), ["max", "min", "max_heap", 'min_heap']
    ):
        if _type:
            pprint(f"{case.__name__}, {_type}")
            case(_type)
            pprint("=-" * 80)


def main():
    # simple()
    all_cases()


if __name__ == "__main__":
    sys.exit(main())
