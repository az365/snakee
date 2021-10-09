from datetime import datetime
from random import randint
from typing import Optional, Callable, Iterable, Iterator, Generator, Union, Any

NOT_USED = None
_AUTO_VALUE = 'Auto'
DEFAULT_VALUE = _AUTO_VALUE
DEFAULT_RANDOM_LEN = 4
STR_FALSE_SYNONYMS = ('False', 'false', 'None', 'none', 'no', '0', '')


class Auto:
    @staticmethod
    def get_value():
        return _AUTO_VALUE

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
        elif hasattr(other, '__name__'):
            other = other.__name__
        elif hasattr(other, '__class__'):
            other = other.__class__.__name__
        return str(other) == str(self.get_value())

    def __repr__(self):
        return str(self.get_value())

    def __str__(self):
        return str(self.__class__.__name__)


DefaultArgument = Auto

AUTO = Auto()
DEFAULT = AUTO


def update(args, addition=None):
    if addition:
        args = list(args) + (list(addition) if isinstance(addition, Iterable) else [addition])
    if len(args) == 1 and isinstance(args[0], (list, tuple, set)):
        args = args[0]
    return args


def apply(func: Callable, *args, **kwargs):
    return func(*args, **kwargs)


def simple_acquire(current, default):
    if current == AUTO:
        return default
    else:
        return current


def delayed_acquire(current, func, *args, **kwargs):
    if current == AUTO:
        assert isinstance(func, Callable)
        return apply(func, *args, **kwargs)
    else:
        return current


def acquire(current, default, *args, delayed=False, **kwargs):
    if delayed or args or kwargs:
        return delayed_acquire(current, func=default, *args, **kwargs)
    else:
        return simple_acquire(current, default)


# @deprecated_with_alternative('arg.simple_acquire(*args, **kwargs)')
def simple_undefault(*args, **kwargs):
    return simple_acquire(*args, **kwargs)


# @deprecated_with_alternative('arg.delayed_acquire(*args, **kwargs)')
def delayed_undefault(*args, **kwargs):
    return delayed_acquire(*args, **kwargs)


# @deprecated_with_alternative('arg.acquire(*args, **kwargs)')
def undefault(*args, **kwargs):
    return acquire(*args, **kwargs)


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


def get_names(iterable: Union[Iterable, Any, None]) -> Union[list, Any]:
    if isinstance(iterable, Iterable) and not isinstance(iterable, str):
        return [get_name(i) for i in iterable]
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
    if isinstance(obj, (tuple, list, set)):
        return len(obj)
    else:
        return default


def is_absolute_path(path: str) -> bool:
    return path.startswith('/') or path.startswith('\\') or ':' in path


def is_defined(obj) -> bool:
    if obj is None:
        result = False
    elif obj in (AUTO, AUTO.get_value(), str(AUTO)):
        result = False
    elif hasattr(obj, 'get_value'):
        result = is_defined(obj.get_value())
    elif hasattr(obj, 'get_name'):
        try:
            name = obj.get_name()
            result = not (name is None or name in (AUTO, _AUTO_VALUE))
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


def any_to_bool(value):
    if isinstance(value, str):
        return value not in STR_FALSE_SYNONYMS
    else:
        return bool(value)


def safe_converter(converter, default_value=0):
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
    return func
