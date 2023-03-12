from typing import Callable, NoReturn

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_str_from_args_kwargs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .arguments import get_str_from_args_kwargs


def get_loc_message(msg: str, obj=None, args=None, kwargs=None) -> str:
    if isinstance(obj, str):
        if args or kwargs:
            str_args_kwargs = get_str_from_args_kwargs(*args, **kwargs)
            name = f'{obj}({str_args_kwargs}'
        else:
            name = obj
    elif isinstance(obj, Callable):
        method_name = obj.__name__
        try:
            cls_name = obj.__self__.__class__.__name__
            name = f'{cls_name}.{method_name}()'
        except AttributeError:
            name = f'{method_name}()'
    else:
        name = repr(obj)
    return f'{name}: {msg}'


def raise_value_error(msg: str, obj=None, args=None, kwargs=None) -> NoReturn:
    msg = get_loc_message(msg, obj=obj, args=args, kwargs=kwargs)
    raise ValueError(msg)
