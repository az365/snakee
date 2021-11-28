from typing import Optional, Union, Callable, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        mappers as ms,
    )
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        mappers as ms,
    )
    from ..primary import numeric as nm

Array = Union[list, tuple]


def is_in(*list_values) -> Callable:
    list_values = arg.update(list_values)

    def func(value: Any) -> bool:
        return value in list_values
    return func


def not_in(*list_values) -> Callable:
    list_values = arg.update(list_values)

    def func(value: Any) -> bool:
        return value not in list_values
    return func


def uniq(save_order: bool = True) -> Callable:
    def func(array: Iterable) -> list:
        if isinstance(array, Iterable):
            result = list()
            for i in array:
                if i not in result:
                    result.append(i)
            return result
    if save_order:
        return func
    else:
        return lambda a: sorted(set(a))


def count_uniq() -> Callable:
    def func(array: Iterable) -> int:
        a = uniq()(array)
        if isinstance(a, list):
            return len(a)
    return func


def count() -> Callable:
    def func(a: Array) -> int:
        return len(a)
    return func


def distinct() -> Callable:
    return uniq()


def elem_no(position: int, default=None) -> Callable:
    def func(array: Array):
        elem_count = len(array)
        if isinstance(array, (list, tuple)) and -elem_count <= position < elem_count:
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


def unfold_lists(fields, number_field='n', default_value=0) -> Callable:
    fields = arg.get_names(fields)

    def func(record: dict) -> Iterable:
        yield from ms.unfold_lists(record, fields=fields, number_field=number_field, default_value=default_value)
    return func


def compare_lists(a_field='a_only', b_field='b_only', ab_field='common', as_dict=True) -> Callable:
    def func(list_a: Array, list_b: Array) -> Union[dict, list, tuple]:
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


def list_minus(save_order: bool = True) -> Callable:
    def func(list_a: Iterable, list_b: Array) -> list:
        return [i for i in list_a if i not in list_b]
    if save_order:
        return func
    else:
        return lambda a, b: sorted(set(a) - set(b))


def values_not_none() -> Callable:
    def func(a: Iterable) -> list:
        return [v for v in a if nm.is_defined(v)]
    return func


def defined_values() -> Callable:
    return values_not_none()


def nonzero_values() -> Callable:
    def func(a: Iterable) -> list:
        return [v for v in a if nm.is_nonzero(v)]
    return func


def numeric_values() -> Callable:
    def func(a: Iterable) -> list:
        return [v for v in a if nm.is_numeric(v)]
    return func


def shift_right(shift: int, default: Any = 0, save_count: bool = True) -> Callable:
    def func(a: Iterable) -> list:
        list_a = list(a)
        if shift == 0:
            return list_a
        count_a = len(list_a)
        if abs(shift) > count_a and save_count:
            addition = [default] * count_a
        else:
            addition = [default] * abs(shift)
        if shift > 0:
            result = addition + list_a
            if save_count:
                result = result[:count_a]
        else:  # shift < 0
            if count_a > shift:
                result = list_a[-shift:]
            else:
                result = list()
            if save_count:
                result += addition
        return result
    return func


def mean(round_digits: Optional[int] = None, default: Any = None, safe: bool = True) -> Callable:
    def func(a) -> float:
        if safe:
            a = numeric_values()(a)
        value = nm.mean(a, default=default, safe=False)
        if value is not None:
            if nm.is_numeric(value):
                if round_digits is not None:
                    value = round(value, round_digits)
                value = float(value)
        return value
    return func


def top(count: int = 10, output_values: bool = False) -> Callable:
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


def hist(as_list: bool = True, sort_by_count: bool = False, sort_by_name: bool = False) -> Callable:
    def func(array: Iterable) -> Union[dict, list]:
        dict_hist = dict()
        for i in array:
            dict_hist[i] = dict_hist.get(i, 0) + 1
        if as_list:
            list_hist = [(k, v) for k, v in dict_hist.items()]
            if sort_by_count:
                return sorted(list_hist, key=lambda p: p[1])
            elif sort_by_name:
                return sorted(list_hist, key=lambda p: p[0])
            else:
                return list_hist
        else:
            return dict_hist
    return func
