from functools import wraps
from typing import Optional

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base import base_classes as bs
    from loggers.logger_interface import LoggerInterface
    from loggers.fallback_logger import FallbackLogger
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base import base_classes as bs
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


def deprecated(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        message = 'Method {}.{}() is deprecated.'
        _get_logger().warning(message.format(func.__module__, func.__name__))
        return func(*args, **kwargs)
    return new_func


def deprecated_with_alternative(alternative):
    def _deprecated(func):
        @wraps(func)
        def new_func(*args, **kwargs):
            message = 'Method {}.{}() is deprecated, use {} instead.'
            _get_logger().warning(message.format(func.__module__, func.__name__, alternative))
            return func(*args, **kwargs)
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
