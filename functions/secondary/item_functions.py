from typing import Optional, Callable, Iterable, Union, Any
import sys
import json
import csv

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.functions.arguments import update
    from base.constants.chars import TAB_CHAR
    from content.items.item_type import ItemType
    from content.items.item_getters import get_composite_key
    from content.selection import selection_functions as sf
    from functions.primary import items as it
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...base.functions.arguments import update
    from ...base.constants.chars import TAB_CHAR
    from ...content.items.item_type import ItemType
    from ...content.items.item_getters import get_composite_key
    from ...content.selection import selection_functions as sf
    from ..primary import items as it

max_int = sys.maxsize
while True:  # To prevent _csv.Error: field larger than field limit (131072)
    try:  # decrease the max_int value by factor 10 as long as the OverflowError occurs.
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)


def composite_key(*functions, item_type: ItemType = ItemType.Auto) -> Callable:
    key_functions = update(functions)

    def _composite_key(item) -> tuple:
        return get_composite_key(item=item, keys_descriptions=key_functions, item_type=item_type)
    return _composite_key


def value_by_key(key, default=None) -> Callable:
    def _value_by_key(item):
        if isinstance(item, dict):
            return item.get(key, default)
        elif isinstance(item, (list, tuple)):
            return item[key] if isinstance(key, int) and 0 <= key <= len(item) else None
    return _value_by_key


def values_by_keys(keys, default=None) -> Callable:
    def _values_by_keys(item) -> list:
        return [value_by_key(k, default)(item) for k in keys]
    return _values_by_keys


def value_by_field(field, item_type: ItemType, struct=None, default=None) -> Callable:
    return item_type.get_field_getter(field, struct=struct, default=default)


def is_in_sample(sample_rate, sample_bucket=1, as_str=True, hash_func=hash) -> Callable:
    def _is_in_sample(elem_id) -> bool:
        if as_str:
            elem_id = str(elem_id)
        return hash_func(elem_id) % sample_rate == sample_bucket
    return _is_in_sample


def same() -> Callable:
    def _same(item):
        return item
    return _same


def merge_two_items(default_right_name: str = '_right') -> Callable:
    def _merge_two_items(first, second):
        return it.merge_two_items(first=first, second=second, default_right_name=default_right_name)
    return _merge_two_items


def items_to_dict(
        key_func: Optional[Callable] = None,
        value_func: Optional[Callable] = None,
        get_distinct: bool = False,
) -> Callable:
    def _items_to_dict(
            items: Iterable,
            key_function: Optional[Callable] = None,
            value_function: Optional[Callable] = None,
            of_lists: bool = False,
    ) -> dict:
        return it.items_to_dict(
            items,
            key_function=key_func or key_function,
            value_function=value_func or value_function,
            of_lists=get_distinct or of_lists,
        )
    return _items_to_dict


def json_dumps(*args, **kwargs) -> Callable:
    def _json_dumps(a: Any) -> str:
        return json.dumps(a, *args, **kwargs)
    return _json_dumps


def json_loads(default=None, skip_errors: bool = False) -> Callable:
    def _json_loads(line: str) -> Any:
        try:
            return json.loads(line)
        except json.JSONDecodeError as err:
            if default is not None:
                return default
            elif not skip_errors:
                raise json.JSONDecodeError(err.msg, err.doc, err.pos)
    return _json_loads


def csv_dumps(delimiter: str = TAB_CHAR, reversible: bool = True) -> Callable:
    func = repr if reversible else str

    def _csv_dumps(row: Union[list, tuple]) -> str:
        return delimiter.join([func(c) for c in row])
    return _csv_dumps


def csv_loads(delimiter: Union[str, Auto, None] = AUTO) -> Callable:
    reader = csv_reader(delimiter=delimiter)

    def _csv_loads(line: str) -> Union[list, tuple]:
        for row in reader([line]):
            return row
    return _csv_loads


def csv_reader(delimiter: Union[str, Auto, None] = AUTO, *args, **kwargs) -> Callable:
    if Auto.is_defined(delimiter):
        return lambda a: csv.reader(a, delimiter=delimiter, *args, **kwargs)
    else:
        return csv.reader
