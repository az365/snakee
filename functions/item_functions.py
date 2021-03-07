from typing import Callable

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        selection as sf,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        selection as sf,
    )


def composite_key(*functions) -> Callable:
    key_functions = arg.update(functions)

    def func(item) -> tuple:
        return sf.get_composite_key(item=item, keys_descriptions=key_functions)
    return func


def value_by_key(key, default=None) -> Callable:
    def func(item):
        if isinstance(item, dict):
            return item.get(key, default)
        elif isinstance(item, (list, tuple)):
            return item[key] if isinstance(key, int) and 0 <= key <= len(item) else None
    return func


def values_by_keys(keys, default=None) -> Callable:
    def func(item) -> list:
        return [value_by_key(k, default)(item) for k in keys]
    return func


def is_in_sample(sample_rate, sample_bucket=1, as_str=True, hash_func=hash) -> Callable:
    def func(elem_id) -> bool:
        if as_str:
            elem_id = str(elem_id)
        return hash_func(elem_id) % sample_rate == sample_bucket
    return func
