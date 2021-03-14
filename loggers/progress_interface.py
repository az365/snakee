from abc import ABC, abstractmethod
from typing import Union, Iterable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers.extended_logger_interface import ExtendedLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .extended_logger_interface import ExtendedLoggerInterface

Logger = Union[ExtendedLoggerInterface, arg.DefaultArgument]


class ProgressInterface(ABC):
    @abstractmethod
    def get_logger(self) -> Logger:
        pass

    @abstractmethod
    def get_selection_logger(self, name=arg.DEFAULT) -> Logger:
        pass

    @abstractmethod
    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=arg.DEFAULT):
        pass

    @abstractmethod
    def log_selection_batch(self, level=arg.DEFAULT, reset_after=True):
        pass

    @abstractmethod
    def is_started(self) -> bool:
        pass

    @abstractmethod
    def is_finished(self) -> bool:
        pass

    @abstractmethod
    def get_percent(self, round_digits=1, default_value='UNK') -> str:
        pass

    @abstractmethod
    def evaluate_share(self) -> float:
        pass

    @abstractmethod
    def evaluate_speed(self) -> int:
        pass

    @abstractmethod
    def update_now(self, cur):
        pass

    @abstractmethod
    def update(self, position, step=None, message=None):
        pass

    @abstractmethod
    def start(self, position=0):
        pass

    @abstractmethod
    def finish(self, position=None, log_selection_batch=True):
        pass

    @abstractmethod
    def iterate(self, items, name=None, expected_count=None, step=arg.DEFAULT, log_selection_batch=True) -> Iterable:
        pass
