from typing import Callable
from inspect import isclass

_AUTO_VALUE = 'Auto'


# @deprecated
class Auto:
    @staticmethod
    def get_value():
        return _AUTO_VALUE

    @classmethod
    def is_auto(cls, other, check_name: bool = True) -> bool:
        if hasattr(other, 'get_value'):
            try:
                other = other.get_value()
            except TypeError:
                pass
        elif hasattr(other, 'get_name') and check_name:
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
        return str(other) == str(cls.get_value())

    @classmethod
    def is_not_set(cls, other) -> bool:
        return other is None

    # @deprecated
    @classmethod
    def is_defined(cls, obj, check_name: bool = True) -> bool:
        if cls.is_not_set(obj):
            result = False
        elif cls.is_auto(obj, check_name=check_name):
            result = False
        elif hasattr(obj, 'is_defined') and not isclass(obj):
            result = obj.is_defined()
        elif hasattr(obj, 'get_value'):
            value = obj.get_value()
            result = cls.is_defined(value)
        elif hasattr(obj, 'get_name') and check_name:
            try:
                name = obj.get_name()
                result = not (cls.is_not_set(name) or cls.is_defined(cls))
            except TypeError:
                result = True
        elif hasattr(obj, 'value'):
            result = cls.is_defined(obj.value)
        else:
            result = obj is not None
        return result

    def __eq__(self, other):
        return self.is_auto(other)

    def __hash__(self):
        return hash(self.get_value())

    def __repr__(self):
        return str(self.get_value())

    def __str__(self):
        return str(self.__class__.__name__)

    @classmethod
    def simple_acquire(cls, current, default):
        if cls.is_auto(current):
            return default
        else:
            return current

    @classmethod
    def delayed_acquire(cls, current, func: Callable, *args, **kwargs):
        if cls.is_auto(current):
            assert isinstance(func, Callable), f'Expected callable, got {func} as {type(func)}'
            return func(*args, **kwargs)
        else:
            return current

    @classmethod
    def acquire(cls, current, default, *args, delayed=False, **kwargs):
        if delayed or args or kwargs:
            return cls.delayed_acquire(current, func=default, *args, **kwargs)
        else:
            return cls.simple_acquire(current, default)

    @classmethod
    def multi_acquire(cls, *values):
        for v in values:
            if not cls.is_auto(v):
                return v
        if values:
            return values[0]
