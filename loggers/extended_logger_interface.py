from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .logger_interface import LoggerInterface


class ExtendedLoggerInterface(LoggerInterface, ABC):
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
    def set_selection_logger(self, **kwargs):
        pass
