from functools import wraps

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from loggers.extended_logger import ExtendedLogger, SingletonLogger, DEFAULT_LOGGER_NAME, DEFAULT_FORMATTER
    from loggers.progress_interface import ProgressInterface, OperationStatus
    from loggers.progress import Progress
    from loggers.detailed_message import DetailedMessage, SelectionError
    from loggers.message_collector import MessageCollector, SelectionMessageCollector, CommonMessageCollector
    from loggers.logging_context_stub import LoggingContextStub
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .logger_interface import LoggerInterface
    from .extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from .extended_logger import ExtendedLogger, SingletonLogger, DEFAULT_LOGGER_NAME, DEFAULT_FORMATTER
    from .progress_interface import ProgressInterface, OperationStatus
    from .progress import Progress
    from .detailed_message import DetailedMessage, SelectionError
    from .message_collector import MessageCollector, SelectionMessageCollector, CommonMessageCollector
    from .logging_context_stub import LoggingContextStub

DEFAULT_LOGGING_LEVEL = LoggingLevel.get_default()


def get_method_name(level: LoggingLevel = LoggingLevel.Info):
    if not isinstance(level, LoggingLevel):
        level = LoggingLevel(level)
    return level.get_method_name()


def get_logger(name=DEFAULT_LOGGER_NAME, level=DEFAULT_LOGGING_LEVEL, context=None):
    if name == DEFAULT_LOGGER_NAME:
        return SingletonLogger(name=name, level=level, context=context)
    else:
        return ExtendedLogger(name=name, level=level, context=context)


def get_base_logger(name=DEFAULT_LOGGER_NAME, level=DEFAULT_LOGGING_LEVEL, formatter=DEFAULT_FORMATTER):
    return ExtendedLogger.build_base_logger(name=name, level=level, formatter=formatter)


def get_selection_logger(**kwargs):
    return get_logger().get_selection_logger(**kwargs)


def is_logger(obj, by_methods=False):
    if isinstance(obj, LoggerInterface):
        return True
    elif 'Logger' in obj.__class__.__name__:
        return True
    elif by_methods and hasattr(obj, 'log') and hasattr(obj, 'warning'):
        return True
    else:
        return False


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
