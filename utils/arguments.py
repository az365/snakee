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
        args = list(args) + (list(addition) if isinstance(addition, Iterable) else [addition])
    if len(args) == 1 and isinstance(args[0], (list, tuple, set)):
        args = args[0]
    return args


def apply(func: Callable, *args, **kwargs):
    return func(*args, **kwargs)


def delayed_undefault(current, func, *args, **kwargs):
    if current == DEFAULT:
        assert isinstance(func, Callable)
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


def get_name(obj) -> Union[str, int]:
    if hasattr(obj, 'get_name'):
        return obj.get_name()
    elif hasattr(obj, 'name'):
        return obj.name
    elif hasattr(obj, '__name__'):
        return obj.__name__
    elif isinstance(obj, int):
        return obj
    else:
        return str(obj)


def get_names(iterable: Optional[Iterable]) -> list:
    if iterable:
        return [get_name(i) for i in iterable]
    else:
        return list()


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
        try:
            name = obj.get_name()
            result = not (name is None or name in (DEFAULT, DEFAULT_VALUE))
        except TypeError:
            result = True
    elif hasattr(obj, 'value'):
        result = is_defined(obj.value)
    else:
        result = bool(obj)
    return result


def is_mask(string: str, count=None, placeholder: str = '*') -> bool:
    if isinstance(string, str):
        if count:
            return len(string.split(placeholder)) == count + 1
        else:
            return len(string.split(placeholder)) > 1


def is_formatter(string: str, count=None) -> bool:
    if isinstance(string, str):
        if count:
            # return min([len(string.split(s)) == count + 1 for s in '{}'])
            return min([is_mask(string, count, placeholder=s) for s in '{}'])
        else:
            # return min([len(string.split(s)) > 1 for s in '{}'])
            return min([is_mask(string, count, placeholder=s) for s in '{}'])
