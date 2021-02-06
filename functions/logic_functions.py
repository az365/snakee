try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg


def maybe(*conditions):
    conditions = arg.update(conditions)

    def func(value):
        for c in conditions:
            if c(value):
                return True
        return False
    return func


def never(*conditions):
    conditions = arg.update(conditions)

    def func(value):
        for c in conditions:
            if c(value):
                return False
        return True
    return func

