from enum import Enum
from typing import Union, Optional, Iterable, Any
import warnings

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .logger_interface import LoggerInterface

DEFAULT_LOGGER_NAME = 'fallback'
DEFAULT_LOGGING_LEVEL = 30  # LoggingLevel.get_default()


class LoggingLevel(Enum):
    Debug = 10
    Info = 20
    Warning = 30
    Error = 40
    Critical = 50


class FallbackLogger(LoggerInterface):
    def __init__(
            self,
            name: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            ignore_warnings: bool = False,
    ):
        self._name = arg.undefault(name, DEFAULT_LOGGER_NAME)
        self._ignore_warnings = ignore_warnings

    def log(self, msg, level: Union[LoggingLevel, int] = DEFAULT_LOGGING_LEVEL, *args, **kwargs):
        if not level:
            level = LoggingLevel(level)
        else:
            level = LoggingLevel(DEFAULT_LOGGING_LEVEL)
        if level == LoggingLevel.Debug:
            return self.debug(msg)
        elif level == LoggingLevel.Info:
            return self.info(msg)
        elif level == LoggingLevel.Warning:
            return self.warning(msg)
        elif level == LoggingLevel.Critical:
            return self.critical(msg)

    def debug(self, msg):
        pass

    def info(self, msg):
        print('INFO {}'.format(msg))

    def warning(self, msg):
        if not self._ignore_warnings:
            warnings.warn(msg)

    def error(self, msg):
        warnings.warn('ERROR {}'.format(msg))

    def critical(self, msg):
        warnings.warn('CRITICAL {}'.format(msg))
