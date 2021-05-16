from typing import Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg


def maybe(*conditions) -> Callable:
    conditions = arg.update(conditions)

    def func(value) -> bool:
        for c in conditions:
            if c(value):
                return True
        return False
    return func


def always(*conditions) -> Callable:
    conditions = arg.update(conditions)

    def func(value) -> bool:
        for c in conditions:
            if not c(value):
                return False
        return True
    return func


def never(*conditions) -> Callable:
    conditions = arg.update(conditions)

    def func(value) -> bool:
        for c in conditions:
            if c(value):
                return False
        return True
    return func

