from abc import ABC, abstractmethod
from enum import Enum
import logging

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.sourced_interface import SourcedInterface
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base.interfaces.sourced_interface import SourcedInterface
    from .logger_interface import LoggerInterface


class LoggingLevel(Enum):
    Debug = logging.DEBUG
    Info = logging.INFO
    Warning = logging.WARNING
    Error = logging.ERROR
    Critical = logging.CRITICAL

    def get_value(self):
        return self.value

    def get_name(self):
        if self == LoggingLevel.Debug:
            return 'debug'
        elif self == LoggingLevel.Info:
            return 'info'
        elif self == LoggingLevel.Warning:
            return 'warning'
        elif self == LoggingLevel.Error:
            return 'error'
        elif self == LoggingLevel.Critical:
            return 'critical'

    def get_method_name(self):
        return self.get_name()

    @classmethod
    def get_default(cls):
        return cls.Warning


class ExtendedLoggerInterface(SourcedInterface, LoggerInterface, ABC):
    @staticmethod
    def is_common_logger() -> bool:
        pass

    @abstractmethod
    def progress(self, items, name='Progress', count=None, step=arg.DEFAULT, context=arg.DEFAULT):
        pass

    @abstractmethod
    def get_new_progress(self, name, **kwargs):
        pass

    @abstractmethod
    def get_selection_logger(self, **kwargs):
        pass

    @abstractmethod
    def set_selection_logger(self, selection_logger, skip_errors=True):
        pass
