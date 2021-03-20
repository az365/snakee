from datetime import datetime
from random import randint
from typing import Union, Generator, Iterator, Iterable, Callable, Optional

NOT_USED = None
DEFAULT_VALUE = 'DefaultArgument'
DEFAULT_RANDOM_LEN = 4


class DefaultArgument:
    @staticmethod
    def get_value():
        return DEFAULT_VALUE

    def __eq__(self, other):
        if hasattr(other, 'get_value'):
            try:
                other = other.get_value()
            except TypeError:
                pass
        elif hasattr(other, 'get_name'):
            try:
                other = other.get_name()
            except TypeError:
                pass
        elif hasattr(other, 'value'):
            other = other.value
        return str(other) == str(self.get_value())

    def __repr__(self):
        return str(self.get_value())

    def __str__(self):
        return str(self.__class__.__name__)


DEFAULT = DefaultArgument()


def update(args, addition=None):
    if addition:
        args = list(args) + (addition if isinstance(addition, (list, tuple)) else [addition])
    if len(args) == 1 and isinstance(args[0], (list, tuple)):
        args = args[0]
    return args


def apply(func: Callable, *args, **kwargs):
    return func(*args, **kwargs)


def delayed_undefault(current, func, *args, **kwargs):
    if current == DEFAULT:
        return apply(func, *args, **kwargs)
    else:
        return current


def simple_undefault(current, default):
    if current == DEFAULT:
        return default
    else:
        return current


def undefault(current, default, *args, delayed=False, **kwargs):
    if delayed or args or kwargs:
        return delayed_undefault(current, func=default, *args, **kwargs)
    else:
        return simple_undefault(current, default)


def get_list(arg: Iterable) -> list:
    if isinstance(arg, Iterable):
        return list(arg)
    elif arg:
        return [arg]
    else:
        return []


def get_generated_name(prefix='snakee', include_random: Union[bool, int] = DEFAULT_RANDOM_LEN, include_datetime=True):
    name_parts = [prefix]
    if include_random:
        if not isinstance(include_random, int):
            include_random = DEFAULT_RANDOM_LEN
        random = randint(0, 10 ** include_random)
        template = '{:0' + str(include_random) + '}'
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
    if isinstance(obj, (tuple, list, set)):
        return len(obj)
    else:
        return default


def is_absolute_path(path: str) -> bool:
    return path.startswith('/') or path.startswith('\\') or ':' in path


def is_defined(obj) -> bool:
    if obj is None:
        result = False
    elif obj in (DEFAULT, DEFAULT.get_value(), str(DEFAULT)):
        result = False
    elif hasattr(obj, 'get_value'):
        result = is_defined(obj.get_value())
    elif hasattr(obj, 'get_name'):
        result = is_defined(obj.get_name())
    elif hasattr(obj, 'value'):
        result = is_defined(obj.value)
    else:
        result = bool(obj)
    return result
