from typing import Optional, Callable, Iterable, Sized, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.functions.arguments import get_names, update
    from utils.decorators import sql_compatible
    from content.items.item_type import ItemType
    from functions.primary import numeric as nm
    from functions.primary import grouping as gr
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...base.functions.arguments import get_names, update
    from ...utils.decorators import sql_compatible
    from ...content.items.item_type import ItemType
    from ..primary import numeric as nm
    from ..primary import grouping as gr

Array = Union[list, tuple]


@sql_compatible
def is_in(*list_values, or_none: bool = False, _as_sql: bool = False) -> Callable:
    if list_values == (None, ) or not list_values:
        if or_none:
            return lambda v: True
        else:
            return lambda v: False
    list_values = update(list_values)
    assert list_values, 'fs.is_in(): non-empty list expected'

    def _is_in(value: Any) -> bool:
        return value in list_values

    def get_sql_repr(field: str) -> str:
        if len(list_values) == 1:
            return '{field} = {value}'.format(field=field, value=repr(list_values[0]))
        else:
            return '{field} IN ({values})'.format(field=field, values=', '.join(map(repr, list_values)))

    return get_sql_repr if _as_sql else _is_in


@sql_compatible
def not_in(*list_values, _as_sql: bool = False) -> Callable:
    list_values = update(list_values)

    def _not_in(value: Any) -> bool:
        return value not in list_values

    def get_sql_repr(field: str) -> str:
        if len(list_values) == 1:
            return '{field} = {value}'.format(field=field, value=repr(list_values[0]))
        else:
            return '{field} NOT IN ({values})'.format(field=field, values=', '.join(map(repr, list_values)))

    return get_sql_repr if _as_sql else _not_in


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


@sql_compatible
def count(default=None, _as_sql: bool = False) -> Callable:
    def func(a: Array) -> int:
        if isinstance(a, Sized):
            return len(a)
        else:
            return default

    def get_sql_repr(field: Optional[str]) -> str:
        return 'COUNT({})'.format(field or '*')

    return get_sql_repr if _as_sql else func


def distinct() -> Callable:
    return uniq()


def elem_no(position: int, default: Any = None) -> Callable:
    def func(array: Array) -> Any:
        elem_count = len(array)
        if isinstance(array, (list, tuple)) and -elem_count <= position < elem_count:
            return array[position]
        else:
            return default
    return func


def subsequence(start: int = 0, end: Optional[int] = None):
    def func(array: Array) -> Array:
        if end is None:
            finish = len(array)
        else:
            finish = end
        return array[start: finish]
    return func


def first(cnt: Optional[int] = None) -> Callable:
    if cnt is None:
        return elem_no(0)
    else:
        return subsequence(0, cnt)


def second() -> Callable:
    return elem_no(1)


def last(cnt: Optional[int] = None) -> Callable:
    if cnt is None:
        return elem_no(-1)
    else:
        return subsequence(0, cnt)


def fold_lists(
        values: Array,
        keys: Optional[Array] = None,
        as_pairs: bool = False,
        skip_missing: bool = False,
        item_type: ItemType = ItemType.Auto,
) -> Callable:
    def func(item) -> Union[dict, tuple]:
        detected_type = item_type
        if not Auto.is_defined(detected_type):
            detected_type = ItemType.detect(item)
        return gr.fold_lists(item, keys, values, as_pairs=as_pairs, skip_missing=skip_missing, item_type=detected_type)
    return func


def unfold_lists(*fields, number_field: str = 'n', default_value: Any = 0) -> Callable:
    if len(fields) == 1:
        if isinstance(fields, Iterable) and not isinstance(fields, str):
            fields = fields[0]
    fields = get_names(fields)

    def func(record: dict) -> Iterable:
        yield from gr.unfold_lists(record, fields=fields, number_field=number_field, default_value=default_value)
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


def top(cnt: int = 10, output_values: bool = False) -> Callable:
    def func(keys, values=None) -> list:
        if values:
            pairs = sorted(zip(keys, values), key=lambda i: i[1], reverse=True)
        else:
            dict_counts = dict()
            for k in keys:
                dict_counts[k] = dict_counts.get(k, 0)
            pairs = sorted(dict_counts.items())
        top_n = pairs[:cnt]
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


def detect_group(**kwargs) -> Callable:
    def func(value: Any) -> str:
        return gr.get_group_name(value, **kwargs)
    return func
