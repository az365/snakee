from typing import Optional, Callable, Iterable, Iterator, Generator, Sized, Union, Type, Any
from inspect import isclass
from datetime import datetime
from random import randint

try:  # Assume we're a submodule in a package.
    from base.constants.chars import (
        KV_DELIMITER, ARG_DELIMITER, ANN_DELIMITER,
        DEFAULT_LINE_LEN, CROP_SUFFIX, SHORT_CROP_SUFFIX,
        FALSE_VALUES,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..constants.chars import (
        KV_DELIMITER, ARG_DELIMITER, ANN_DELIMITER,
        DEFAULT_LINE_LEN, CROP_SUFFIX, SHORT_CROP_SUFFIX,
        FALSE_VALUES,
    )

DEFAULT_RANDOM_LEN = 6
TYPING_PREFIX = 'typing.'
STR_FALSE_SYNONYMS = ['False', 'None', 'none'] + list(FALSE_VALUES)  # 'false', 'no', '0', '0.0', DEFAULT_STR, EMPTY


def update(args, addition=None):
    if addition:
        if isinstance(addition, Iterable) and not isinstance(addition, (list, str)):
            list_addition = list(addition)
        else:
            list_addition = [addition]
        args = list(args) + list_addition
    if len(args) == 1:
        single_arg = args[0]
        if isinstance(single_arg, (list, tuple, set)) and not isinstance(single_arg, str):
            args = single_arg
    return args


def get_list(arg: Iterable) -> list:
    if isinstance(arg, str):
        return [arg]
    elif isinstance(arg, Iterable):
        return list(arg)
    elif arg:
        return [arg]
    else:
        return []


def get_value(obj) -> Union[str, int]:
    if hasattr(obj, 'get_value'):
        return obj.get_value()
    elif hasattr(obj, 'value'):
        return obj.value
    else:
        return obj


def get_name(obj, or_callable: bool = True, or_class: bool = True) -> Union[str, int, Callable]:
    if or_callable and isinstance(obj, Callable):
        return obj
    elif hasattr(obj, 'get_name'):
        try:
            return obj.get_name()
        except TypeError:  # is class (not instanced)
            if or_class:
                return obj
            else:
                return obj.__name__
    elif hasattr(obj, 'name'):
        return obj.name
    elif hasattr(obj, '__name__'):
        return obj.__name__
    elif isinstance(obj, int):
        return obj
    else:
        return str(obj)


def get_names(iterable: Union[Iterable, Any, None], or_callable: bool = True) -> Union[list, Any]:
    if isinstance(iterable, Iterable) and not (isinstance(iterable, str) or hasattr(iterable, '_value_type')):  # Field
        return [get_name(i, or_callable=or_callable) for i in iterable]
    else:
        return iterable


def get_plural(name: str, suffix: str = '_list') -> str:
    return f'{get_name(name)}{suffix}'


def get_generated_name(prefix='snakee', include_random: Union[bool, int] = DEFAULT_RANDOM_LEN, include_datetime=True):
    name_parts = [prefix]
    if include_random:
        random_len = DEFAULT_RANDOM_LEN if isinstance(include_random, bool) else int(include_random)
        random = randint(0, 10 ** random_len)
        template = '{:0' + str(random_len) + '}'
        name_parts.append(template.format(random))
    if include_datetime:
        cur_time = datetime.now().strftime('%y%m%d_%H%M%S')
        name_parts.append(cur_time)
    return '_'.join(name_parts)


def is_generator(obj) -> bool:
    return isinstance(obj, (Generator, Iterator, range))


def is_in_memory(obj) -> bool:
    return not is_generator(obj)


def get_optional_len(obj: Iterable, default=None) -> Optional[int]:
    if isinstance(obj, Sized):
        return len(obj)
    else:
        return default


def get_str_from_args_kwargs(
        *args,
        _arg_delimiter: str = ARG_DELIMITER,  # ', '
        _kv_delimiter: str = KV_DELIMITER,  # '='
        _remove_prefixes: Optional[Iterable] = None,
        _kwargs_order: Optional[Iterable] = None,
        **kwargs
) -> str:
    list_str_from_kwargs = list()
    if _kwargs_order is None:
        ordered_kwargs_items = kwargs.items()
    else:
        _kwargs_order = list(_kwargs_order)
        ordered_kwargs_items = list()
        for k in _kwargs_order:
            if k in kwargs:
                ordered_kwargs_items.append((k, kwargs[k]))
        for k, v in kwargs.items():
            if k not in _kwargs_order:
                ordered_kwargs_items.append((k, v))
    for k, v in ordered_kwargs_items:
        if isclass(v):
            v_str = v.__name__
        else:
            v_str = repr(v)
        for prefix in _remove_prefixes or []:
            if v_str.startswith(prefix):
                v_str = v_str[len(prefix):]
        list_str_from_kwargs.append(f'{k}{_kv_delimiter}{v_str}')
    list_str_from_args = [str(i) for i in args]
    return ', '.join(list_str_from_args + list_str_from_kwargs)


def get_str_from_annotation(class_or_func: Union[Callable, Type], _delimiter: str = ANN_DELIMITER) -> str:
    if isclass(class_or_func):
        func = class_or_func
        name = class_or_func.__name__
    elif isinstance(class_or_func, Callable):
        func = class_or_func
        name = class_or_func.__name__
    elif isinstance(class_or_func, object):
        func = class_or_func.__class__
        name = class_or_func.__class__.__name__
    else:
        raise TypeError
    if hasattr(func, '__annotations__'):
        ann_dict = func.__annotations__
        ann_str = get_str_from_args_kwargs(**ann_dict, _kv_delimiter=_delimiter, _remove_prefixes=[TYPING_PREFIX])
    else:
        ann_str = '*args, **kwargs'
    return f'{name}({ann_str})'


def get_cropped_text(
        text,
        max_len: int = DEFAULT_LINE_LEN,
        crop_suffix: str = CROP_SUFFIX,
        short_crop_suffix: str = SHORT_CROP_SUFFIX,
) -> str:
    text = str(text)
    crop_len = len(crop_suffix)
    if isinstance(max_len, int):  # max_len is not None:
        text_len = len(text)
        if text_len > max_len:
            value_len = max_len - crop_len
            if value_len > 0:
                text = text[:value_len] + crop_suffix
            elif max_len > 1:
                text = text[:max_len - 1] + short_crop_suffix
            else:
                text = text[:max_len]
    return text


def str_to_bool(line: str) -> bool:
    return line not in STR_FALSE_SYNONYMS


def any_to_bool(value) -> bool:
    if isinstance(value, str):
        return str_to_bool(value)
    else:
        return bool(value)
