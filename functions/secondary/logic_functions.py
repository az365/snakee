from typing import Callable

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import update
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.functions.arguments import update


def maybe(*conditions) -> Callable:
    conditions = update(conditions)

    def func_conditioned(value) -> bool:
        for c in conditions:
            if c(value):
                return True
        return False

    def func_simple(*values) -> bool:
        return max(map(bool, values))

    if conditions:
        return func_conditioned
    else:
        return func_simple


def always(*conditions) -> Callable:
    conditions = update(conditions)

    def func_conditioned(value) -> bool:
        for c in conditions:
            if not c(value):
                return False
        return True

    def func_simple(*values) -> bool:
        values = update(values)
        return min(map(bool, values))

    if conditions:
        return func_conditioned
    else:
        return func_simple


def never(*conditions) -> Callable:
    conditions = update(conditions)

    def func_conditioned(value) -> bool:
        for c in conditions:
            if c(value):
                return False
        return True

    def func_simple(value) -> bool:
        return not value

    if conditions:
        return func_conditioned
    else:
        return func_simple
