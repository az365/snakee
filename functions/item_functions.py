try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        items as it,
        selection,
    )


def composite_key(*functions):
    key_functions = arg.update(functions)

    def func(item):
        result = list()
        for f in key_functions:
            if callable(f):
                value = f(item)
            else:
                if it.ItemType.Record.isinstance(item):
                    value = selection.value_from_record(item, f)
                elif it.ItemType.Row.isinstance(item):
                    value = selection.value_from_row(item, f)
                else:
                    value = selection.value_from_any(item, f)
            result.append(value)
        return tuple(result)
    return func


def value_by_key(key, default=None):
    def func(item):
        if isinstance(item, dict):
            return item.get(key, default)
        elif isinstance(item, (list, tuple)):
            return item[key] if isinstance(key, int) and 0 <= key <= len(item) else None
    return func


def values_by_keys(keys, default=None):
    def func(item):
        return [value_by_key(k, default)(item) for k in keys]
    return func


def is_in_sample(sample_rate, sample_bucket=1, as_str=True, hash_func=hash):
    def func(elem_id):
        if as_str:
            elem_id = str(elem_id)
        return hash_func(elem_id) % sample_rate == sample_bucket
    return func
