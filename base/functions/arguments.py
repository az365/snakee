from typing import Optional, Callable, Iterable, Iterator, Generator, Sized, Union, Type, Any
from inspect import isclass
from datetime import datetime
from random import randint

DEFAULT_RANDOM_LEN = 4


def update(args, addition=None):
    if addition:
        args = list(args) + (list(addition) if isinstance(addition, Iterable) else [addition])
    if len(args) == 1 and isinstance(args[0], (list, tuple, set)):
        args = args[0]
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


def get_name(obj, or_callable: bool = True) -> Union[str, int, Callable]:
    if hasattr(obj, 'get_name'):
        return obj.get_name()
    elif hasattr(obj, 'name'):
        return obj.name
    elif isinstance(obj, Callable) and or_callable:
        return obj
    elif hasattr(obj, '__name__'):
        return obj.__name__
    elif isinstance(obj, int):
        return obj
    else:
        return str(obj)


def get_names(iterable: Union[Iterable, Any, None], or_callable: bool = True) -> Union[list, Any]:
    if isinstance(iterable, Iterable) and not isinstance(iterable, str):
        return [get_name(i, or_callable=or_callable) for i in iterable]
    else:
        return iterable


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
        delimiter: str = '=',
        remove_prefixes: Optional[Iterable] = None,
        **kwargs
) -> str:
    list_str_from_kwargs = list()
    for k, v in kwargs.items():
        if isclass(v):
            v_str = v.__name__
        else:
            v_str = v.__repr__()
        # try:
        #     v_str = v.__repr__()
        # except TypeError:
        #     v_str = str(v)
        for prefix in remove_prefixes or []:
            if v_str.startswith(prefix):
                v_str = v_str[len(prefix):]
        list_str_from_kwargs.append('{}{}{}'.format(k, delimiter, v_str))
    list_str_from_args = [str(i) for i in args]
    return ', '.join(list_str_from_args + list_str_from_kwargs)
    # list_str_from_args_kwargs = [str(i) for i in args] + ['{}={}'.format(k, v) for k, v in kwargs.items()]
    # return ', '.join(list_str_from_args_kwargs)


def get_str_from_annotation(class_or_func: Union[Callable, Type]) -> str:
    if isclass(class_or_func):
        func = class_or_func.__init__
        name = class_or_func.__name__
    elif isinstance(class_or_func, Callable):
        func = class_or_func
        name = class_or_func.__name__
    elif isinstance(class_or_func, object):
        func = class_or_func.__class__.__init__
        name = class_or_func.__class__.__name__
    else:
        raise TypeError
    if hasattr(func, '__annotations__'):
        ann_dict = func.__annotations__
        ann_str = get_str_from_args_kwargs(**ann_dict, delimiter=': ', remove_prefixes=['typing.'])
    else:
        ann_str = '*args, **kwargs'
    return '{}({})'.format(name, ann_str)
