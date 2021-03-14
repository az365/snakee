from functools import wraps
import logging

from loggers.extended_logger_interface import LoggingLevel

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers.extended_logger import ExtendedLogger, SingletonLogger
    from loggers.progress import Progress
    from loggers.detailed_message import DetailedMessage, SelectionError
    from loggers.message_collector import MessageCollector, SelectionMessageCollector, CommonMessageCollector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .extended_logger import ExtendedLogger, SingletonLogger
    from .progress import Progress
    from .detailed_message import DetailedMessage, SelectionError
    from .message_collector import MessageCollector, SelectionMessageCollector, CommonMessageCollector

DEFAULT_STEP = 10000
DEFAULT_LOGGER_NAME = 'stream'
DEFAULT_LOGGING_LEVEL = logging.WARNING
DEFAULT_FORMATTER = '%(asctime)s - %(levelname)s - %(message)s'
DEFAULT_LINE_LEN = 127
LONG_LINE_LEN = 600


def get_method_name(level=LoggingLevel.Info):
    if not isinstance(level, LoggingLevel):
        level = LoggingLevel(level)
    if level == LoggingLevel.Debug:
        return 'debug'
    elif level == LoggingLevel.Info:
        return 'info'
    elif level == LoggingLevel.Warning:
        return 'warning'
    elif level == LoggingLevel.Error:
        return 'error'
    elif level == LoggingLevel.Critical:
        return 'critical'


def get_logger(name=DEFAULT_LOGGER_NAME, level=DEFAULT_LOGGING_LEVEL, context=None):
    if name == DEFAULT_LOGGER_NAME:
        return SingletonLogger(name=name, level=level, context=context)
    else:
        return ExtendedLogger(name=name, level=level, context=context)


def get_base_logger(name=DEFAULT_LOGGER_NAME, level=DEFAULT_LOGGING_LEVEL, formatter=DEFAULT_FORMATTER):
    base_logger = logging.getLogger(name)
    base_logger.setLevel(level)
    if not base_logger.handlers:
        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(formatter)
        sh.setFormatter(formatter)
        base_logger.addHandler(sh)
    return base_logger


def get_selection_logger(**kwargs):
    return get_logger().get_selection_logger(**kwargs)


def deprecated(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        message = 'Method {}.{}() is deprecated.'
        get_logger().warning(message.format(func.__module__, func.__name__))
        return func(*args, **kwargs)
    return new_func


def deprecated_with_alternative(alternative):
    def _deprecated(func):
        @wraps(func)
        def new_func(*args, **kwargs):
            message = 'Method {}.{}() is deprecated, use {} instead.'
            get_logger().warning(message.format(func.__module__, func.__name__, alternative))
            return func(*args, **kwargs)
        return new_func
    return _deprecated
