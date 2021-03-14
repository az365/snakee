from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg


class LoggerInterface(ABC):
    @abstractmethod
    def log(self, msg, level, *args, **kwargs):
        pass

    @abstractmethod
    def debug(self, msg):
        pass

    @abstractmethod
    def info(self, msg):
        pass

    @abstractmethod
    def warning(self, msg):
        pass

    @abstractmethod
    def error(self, msg):
        pass

    @abstractmethod
    def critical(self, msg):
        pass
