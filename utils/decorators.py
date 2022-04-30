from functools import wraps
from typing import Type, Callable, Optional, Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from loggers.logger_interface import LoggerInterface
    from loggers.fallback_logger import FallbackLogger
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
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
            raise TypeError('{}: {}'.format(func, e))
    return new_func


def deprecated_with_alternative(alternative):
    def _deprecated(func):
        @wraps(func)
        def new_func(*args, **kwargs):
            template = 'Method {}.{}() is deprecated, use {} instead.'
            message = template.format(func.__module__, func.__name__, alternative)
            _warn(message, category=DeprecationWarning, stacklevel=1)
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


def sql_compatible(func):
    class SqlCompatibleFunction(Callable):
        @wraps(func)
        def __init__(self, *args, **kwargs):
            if '_as_sql' in kwargs:
                kwargs.pop('_as_sql')
            self._name = func.__name__
            self._py_func = func(*args, **kwargs, _as_sql=False)
            self._sql_func = func(*args, **kwargs, _as_sql=True)
            self._repr = '{}({})'.format(self._name, arg.get_str_from_args_kwargs(*args, **kwargs))

        def __call__(self, *args, **kwargs):
            return self._py_func(*args, **kwargs)

        def get_sql_expr(self, *args, **kwargs):
            if isinstance(self, SqlCompatibleFunction):
                return self._sql_func(*args, **kwargs)
            else:
                raise TypeError('function must be initialized before using function.get_sql_expr()') from None

        def __repr__(self):
            return self._repr

    return SqlCompatibleFunction
