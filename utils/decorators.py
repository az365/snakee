from abc import ABC
from typing import Type, Callable, Optional, Union
from functools import wraps
import inspect

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_name, get_str_from_args_kwargs
    from loggers.logger_interface import LoggerInterface
    from loggers.fallback_logger import FallbackLogger
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.functions.arguments import get_name, get_str_from_args_kwargs
    from ..loggers.logger_interface import LoggerInterface
    from ..loggers.fallback_logger import FallbackLogger

_logger = None


def _get_logger(default: Optional[LoggerInterface] = None) -> Optional[LoggerInterface]:
    global _logger
    if not _logger:
        _logger = default if default else FallbackLogger()
    return _logger


def _set_logger(logger: LoggerInterface):
    global _logger
    _logger = logger


def _warn(msg: Union[str, Warning], category: Optional[Type] = None, stacklevel: Optional[int] = 1):
    logger = _get_logger()
    try:
        has_stacklevel_attribute = 'stacklevel' in logger.warning.__annotations__
    except AttributeError:
        has_stacklevel_attribute = True
    if has_stacklevel_attribute:
        if stacklevel is not None:
            stacklevel += 1
        logger.warning(msg, category=category, stacklevel=stacklevel)
    else:
        logger.warning(msg)


def deprecated(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        template = 'Method {}.{}() is deprecated.'
        message = template.format(func.__module__, func.__name__)
        _warn(message, category=DeprecationWarning, stacklevel=1)
        try:
            return func(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f'{get_name(func, or_callable=False)}: {e}')
    return new_func


def deprecated_with_alternative(alternative):
    def _deprecated(func):
        @wraps(func)
        def new_func(*args, **kwargs):
            template = 'Method {}.{}() is deprecated, use {} instead.'
            message = template.format(func.__module__, func.__name__, alternative)
            _warn(message, category=DeprecationWarning, stacklevel=2)
            try:
                return func(*args, **kwargs)
            except TypeError as e:
                raise TypeError('{}: {}'.format(func, e))
        return new_func
    return _deprecated


def singleton(cls):
    @wraps(cls)
    def wrapper(*args, **kwargs):
        if not wrapper.instance:
            wrapper.instance = cls(*args, **kwargs)
        return wrapper.instance
    wrapper.instance = None
    return wrapper


class WrappedFunction(Callable, ABC):
    def __init__(self, py_func: Callable, name: Optional[str] = None, *args, **kwargs):
        self._py_func = py_func
        if name is None:
            name = get_name(py_func, or_callable=False)
        self._name = name
        self._args = args
        self._kwargs = kwargs

    def __call__(self, *args, **kwargs):
        py_func = self.get_py_func()
        return py_func(*args, **kwargs)

    def get_py_func(self) -> Callable:
        return self._py_func

    def get_py_code(self) -> str:
        py_func = self.get_py_func()
        try:
            return inspect.getsource(py_func)
        except TypeError as e:
            return repr(e)

    def get_name(self) -> str:
        return self._name

    def __repr__(self):
        name = self.get_name()
        args_str = get_str_from_args_kwargs(*self._args, **self._kwargs)
        return f'{name}({args_str})'

    def __str__(self):
        cls_name = self.__class__.__name__
        obj_name = repr(self.get_name())
        return f'{cls_name}({obj_name})'


def sql_compatible(func):
    class SqlCompatibleFunction(WrappedFunction):
        @wraps(func)
        def __init__(self, *args, **kwargs):
            if '_as_sql' in kwargs:
                kwargs.pop('_as_sql')
            super().__init__(
                func(*args, **kwargs, _as_sql=False),
                get_name(func, or_callable=False),
                *args, **kwargs,
            )
            self._sql_func = func(*args, **kwargs, _as_sql=True)

        def get_sql_expr(self, *args, **kwargs):
            if isinstance(self, SqlCompatibleFunction):
                return self._sql_func(*args, **kwargs)
            else:
                raise TypeError('function must be initialized before using function.get_sql_expr()') from None

    return SqlCompatibleFunction
