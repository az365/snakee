from typing import Union, Callable, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        mappers as ms,
        numeric as nm,
    )
    from functions import basic_functions as bf
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        mappers as ms,
        numeric as nm,
    )
    from . import basic_functions as bf


def is_in(*list_values) -> Callable:
    list_values = arg.update(list_values)

    def func(value) -> bool:
        return value in list_values
    return func


def not_in(*list_values) -> Callable:
    list_values = arg.update(list_values)

    def func(value) -> bool:
        return value not in list_values
    return func


def is_ordered(reverse: bool = False, including: bool = True) -> Callable:
    def func(previous, current) -> bool:
        if current == previous:
            return including
        elif reverse:
            return bf.safe_more_than(current)(previous)
        else:
            return bf.safe_more_than(previous)(current)
    return func


def elem_no(position: int, default=None) -> Callable:
    def func(array: Union[list, tuple]):
        count = len(array)
        if isinstance(array, (list, tuple)) and -count <= position < count:
            return array[position]
        else:
            return default
    return func


def first() -> Callable:
    return elem_no(0)


def second() -> Callable:
    return elem_no(1)


def last() -> Callable:
    return elem_no(-1)


def uniq() -> Callable:
    def func(array: Iterable) -> list:
        if isinstance(array, Iterable):
            result = list()
            for i in array:
                if i not in result:
                    result.append(i)
            return result
    return func


def count_uniq() -> Callable:
    def func(array: Iterable) -> int:
        a = uniq()(array)
        if isinstance(a, list):
            return len(a)
    return func


def distinct() -> Callable:
    return uniq()


def unfold_lists(fields, number_field='n', default_value=0) -> Callable:
    fields = arg.get_names(fields)

    def func(record: dict) -> Iterable:
        yield from ms.unfold_lists(record, fields=fields, number_field=number_field, default_value=default_value)
    return func


def compare_lists(a_field='a_only', b_field='b_only', ab_field='common', as_dict=True) -> Callable:
    def func(list_a: Iterable, list_b: Iterable) -> Union[dict, list, tuple]:
        items_common, items_a_only, items_b_only = list(), list(), list()
        for item in list_a:
            if item in list_b:
                items_common.append(item)
            else:
                items_a_only.append(item)
        for item in list_b:
            if item not in list_a:
                items_b_only.append(item)
        result = ((a_field, items_a_only), (b_field, items_b_only), (ab_field, items_common))
        if as_dict:
            return dict(result)
        else:
            return result
    return func


def list_minus() -> Callable:
    def func(list_a: Iterable, list_b: Iterable) -> list:
        return [i for i in list_a if i not in list_b]
    return func


def values_not_none() -> Callable:
    def func(a: Iterable) -> list:
        return [v for v in a if bf.not_none()(v)]
    return func


def defined_values() -> Callable:
    def func(a: Iterable) -> list:
        return [v for v in a if nm.is_defined(v)]
    return func


def nonzero_values() -> Callable:
    def func(a: Iterable) -> list:
        return [v for v in a if nm.is_nonzero(v)]
    return func


def numeric_values() -> Callable:
    def func(a: Iterable) -> list:
        return [v for v in a if nm.is_numeric(v)]
    return func


def shift_right(shift: int, default: Any = 0, save_count=True) -> Callable:
    def func(a: Iterable) -> list:
        list_a = list(a)
        if shift == 0:
            return list_a
        count = len(list_a)
        if abs(shift) > count and save_count:
            addition = [default] * count
        else:
            addition = [default] * abs(shift)
        if shift > 0:
            result = addition + list_a
            if save_count:
                result = result[:count]
        else:  # shift < 0
            if count > shift:
                result = list_a[-shift:]
            else:
                result = list()
            if save_count:
                result += addition
        return result
    return func


def mean() -> Callable:
    def func(a) -> float:
        return nm.mean(numeric_values()(a))
    return func


def top(count=10, output_values=False) -> Callable:
    def func(keys, values=None) -> list:
        if values:
            pairs = sorted(zip(keys, values), key=lambda i: i[1], reverse=True)
        else:
            dict_counts = dict()
            for k in keys:
                dict_counts[k] = dict_counts.get(k, 0)
            pairs = sorted(dict_counts.items())
        top_n = pairs[:count]
        if output_values:
            return top_n
        else:
            return [i[0] for i in top_n]
    return func
