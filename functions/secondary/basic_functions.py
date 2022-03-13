from typing import Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..primary import numeric as nm

ZERO_VALUES = None, 'None', '', '-', 0


def same() -> Callable:
    def _same(item):
        return item
    return _same


def partial(function, *args, **kwargs) -> Callable:
    def _partial(item: Any) -> Any:
        return function(item, *args, **kwargs)
    return _partial


def const(value: Any) -> Callable:
    def _const(*_) -> Any:
        return value
    return _const


def defined() -> Callable:
    def _defined(value: Any) -> bool:
        return value is not None
    return _defined


def is_none() -> Callable:
    def _is_none(value: Any) -> bool:
        return nm.is_none(value)
    return _is_none


def not_none() -> Callable:
    def _not_none(value: Any) -> bool:
        return nm.is_defined(value)
    return _not_none


def nonzero(zero_values: Union[set, list, tuple] = ZERO_VALUES) -> Callable:
    def _nonzero(value: Any) -> bool:
        if nm.is_defined(value):
            return value not in zero_values
    return _nonzero


def equal(*args) -> Callable:
    if len(args) == 0:
        benchmark = None
        is_benchmark_defined = False
    elif len(args) == 1:
        is_benchmark_defined = True
        benchmark = args[0]
    else:
        raise ValueError('fs.equals() accepts 0 or 1 argument, got {}'.format(args))

    def _equal(*a) -> bool:
        if len(a) == 1 and is_benchmark_defined:
            value, other = a[0], benchmark
        elif len(a) == 2 and not is_benchmark_defined:
            value, other = a
        else:
            raise ValueError('fs.equal() accepts 1 benchmark and 1 value, got benchmark={}, value={}'.format(args, a))
        return value == other
    return _equal


def not_equal(*args) -> Callable:
    _func = equal(*args)

    def _not_equal(*a) -> bool:
        return not _func(*a)
    return _not_equal


def less_than(other: Any, including: bool = False) -> Callable:
    def _less_than(value: Any) -> bool:
        if including:
            return value <= other
        else:
            return value < other
    return _less_than


def more_than(other: Any, including: bool = False) -> Callable:
    def _more_than(value: Any) -> bool:
        if including:
            return value >= other
        else:
            return value > other
    return _more_than


def at_least(number: Any) -> Callable:
    return more_than(number, including=True)


def safe_more_than(other: Any, including: bool = False) -> Callable:
    def _safe_more_than(value) -> bool:
        first, second = value, other
        if type(first) != type(second):
            first_is_numeric = isinstance(first, nm.NUMERIC_TYPES)
            second_is_numeric = isinstance(second, nm.NUMERIC_TYPES)
            if first_is_numeric:
                if second_is_numeric:
                    first = float(first)
                    second = float(second)
                else:  # second is not numeric
                    return True
            elif second_is_numeric:
                return False
            else:
                first = str(type(first))
                second = str(type(second))
        try:
            if including:
                return first >= second
            else:
                return first > second
        except TypeError as e:
            if isinstance(first, Iterable) and isinstance(second, Iterable):
                for f, s in zip(first, second):
                    if safe_more_than(s, including=including)(f):
                        return True
                return False
            else:
                raise TypeError('{}: {} vs {}'.format(e, first, second))
    return _safe_more_than


def is_ordered(reverse: bool = False, including: bool = True) -> Callable:
    def _is_ordered(previous, current) -> bool:
        if current == previous:
            return including
        elif reverse:
            return safe_more_than(current)(previous)
        else:
            return safe_more_than(previous)(current)
    return _is_ordered


def between(min_value, max_value, including=False) -> Callable:
    def _between(value) -> bool:
        if including:
            return min_value <= value <= max_value
        else:
            return min_value < value < max_value
    return _between


def not_between(min_value, max_value, including=False) -> Callable:
    func_between = between(min_value, max_value, including)

    def _not_between(value) -> bool:
        return not func_between(value)
    return _not_between


def apply_dict(dictionary, default=None) -> Callable:
    def _apply_dict(key):
        return dictionary.get(key, default)
    return _apply_dict


def acquire(default=None, zero_values: Union[list, tuple] = ZERO_VALUES) -> Callable:
    def _acquire(*values):
        for v in values:
            if v not in zero_values:
                return v
        return default
    return _acquire
