from typing import Callable, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto
    from base.functions.arguments import get_names, get_name, get_value, update
    from functions.primary.text import str_to_bool
    from utils.decorators import deprecated, deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.classes.auto import Auto
    from ..base.functions.arguments import get_names, get_name, get_value, update
    from ..functions.primary.text import str_to_bool
    from .decorators import deprecated, deprecated_with_alternative


@deprecated
def apply(func: Callable, *args, **kwargs):
    return func(*args, **kwargs)


@deprecated_with_alternative('Auto.is_defined()')
def is_defined(obj, check_name: bool = True) -> bool:
    return Auto.is_defined(obj, check_name=check_name)


@deprecated_with_alternative('Auto.simple_acquire()')
def simple_acquire(current, default):
    return Auto.simple_acquire(current, default)


@deprecated_with_alternative('Auto.delayed_acquire()')
def delayed_acquire(current, func: Callable, *args, **kwargs):
    return Auto.delayed_acquire(current, func, *args, **kwargs)


@deprecated_with_alternative('Auto.acquire()')
def acquire(current, default, delayed=False, *args, **kwargs):
    return Auto.acquire(current, default, delayed=delayed, *args, **kwargs)


@deprecated_with_alternative('arg.simple_acquire(*args, **kwargs)')
def simple_undefault(*args, **kwargs):
    return simple_acquire(*args, **kwargs)


@deprecated_with_alternative('arg.delayed_acquire(*args, **kwargs)')
def delayed_undefault(*args, **kwargs):
    return delayed_acquire(*args, **kwargs)


@deprecated_with_alternative('arg.acquire(*args, **kwargs)')
def undefault(*args, **kwargs):
    return acquire(*args, **kwargs)


# @deprecated
def any_to_bool(value) -> bool:
    if isinstance(value, str):
        return str_to_bool(value)
    else:
        return bool(value)


# @deprecated
def safe_converter(converter: Callable, default_value: Any = 0, eval_allowed: bool = False) -> Callable:
    def func(value):
        if value is None or value == '':
            return default_value
        else:
            try:
                return converter(value)
            except ValueError:
                return default_value
            except NameError:
                return default_value
            except TypeError as e:
                converter_name = converter.__name__
                if converter_name == 'eval':
                    if eval_allowed:
                        return eval(str(value))
                    else:
                        return value
                else:
                    raise TypeError(f'{e}: {converter_name}({value})')
    return func
