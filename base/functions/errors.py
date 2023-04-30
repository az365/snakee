from typing import Optional, Callable, Iterable, Union, Any, NoReturn
from inspect import getframeinfo, stack

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Array, Line, Count
    from base.constants.chars import DEFAULT_ITEMS_DELIMITER
    from base.functions.arguments import get_name, get_str_from_args_kwargs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Array, Line, Count
    from ..constants.chars import DEFAULT_ITEMS_DELIMITER
    from .arguments import get_name, get_str_from_args_kwargs

Caller = Union[Callable, Line, Count]
Args = Optional[Array]
Kwargs = Optional[dict]


def get_loc_message(msg: str, caller: Caller = None, args: Args = None, kwargs: Kwargs = None) -> str:
    if caller is None or isinstance(caller, int):
        stacklevel = abs(caller or 1) + 1
        caller_info = getframeinfo(stack()[stacklevel][0])
        name = f'{caller_info.function}()'
    elif isinstance(caller, Line):
        if args or kwargs:
            str_args_kwargs = get_str_from_args_kwargs(*args, **kwargs)
            name = f'{caller}({str_args_kwargs}'
        else:
            name = caller
    elif isinstance(caller, Callable):
        method_name = caller.__name__
        try:
            cls_name = caller.__self__.__class__.__name__
            name = f'{cls_name}.{method_name}()'
        except AttributeError:
            name = f'{method_name}()'
    else:
        name = repr(caller)
    return f'{name}: {msg}'


def raise_value_error(
        msg: str,
        caller: Caller = None,
        expected: Args = None,
        args: Args = None,
        kwargs: Kwargs = None,
) -> NoReturn:
    msg = get_loc_message(msg, caller=caller, args=args, kwargs=kwargs)
    raise ValueError(msg)


def get_type_err_msg(
        got: Any,
        expected: Union[type, str, tuple],
        arg: str,
        caller: Caller = None,
        args: Args = None,
        kwargs: Kwargs = None,
) -> str:
    got_repr = repr(got)
    got_type = type(got).__name__
    expected_type = _get_type_name(expected)
    msg = f'expected {arg} as {expected_type}; got {got_repr} as {got_type}'
    msg = get_loc_message(msg, caller=caller or 1, args=args, kwargs=kwargs)
    return msg


def raise_type_error(
        got: Any,
        expected: Union[type, str, tuple],
        arg: str,
        caller: Caller = None,
        args: Args = None,
        kwargs: Kwargs = None,
) -> NoReturn:
    msg = get_type_err_msg(got, expected=expected, arg=arg, caller=caller, args=args, kwargs=kwargs)
    raise TypeError(msg)


def _get_type_name(
        obj_type: Union[type, str, tuple],
        delimiter: str = DEFAULT_ITEMS_DELIMITER,
        add_scope: bool = False,
) -> str:
    if isinstance(obj_type, type):
        type_name = obj_type.__name__
    elif isinstance(obj_type, str):
        type_name = obj_type
    elif isinstance(obj_type, Iterable):
        type_name = delimiter.join(map(_get_type_name, obj_type))
        if add_scope:
            type_name = f'({type_name})'
    else:
        type_name = type(obj_type).__name__
    return type_name
