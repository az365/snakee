from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Union, Iterable, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract.tree_item import TreeInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base.abstract.tree_item import TreeInterface
    from .extended_logger_interface import ExtendedLoggerInterface

Logger = Union[ExtendedLoggerInterface, arg.DefaultArgument]


class OperationStatus(Enum):
    New = 'new'
    InProgress = 'in_progress'
    Done = 'done'

    def get_name(self):
        return self.value


class ProgressInterface(TreeInterface, ABC):
    @abstractmethod
    def get_logger(self) -> Logger:
        pass

    @abstractmethod
    def get_position(self) -> int:
        pass

    @abstractmethod
    def set_position(self, position: int, inplace: bool) -> Optional[TreeInterface]:
        pass

    @abstractmethod
    def get_selection_logger(self, name=arg.DEFAULT) -> Logger:
        pass

    @abstractmethod
    def log(
            self, msg: str,
            level: Union[int, arg.DefaultArgument] = arg.DEFAULT,
            end: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ) -> NoReturn:
        pass

    @abstractmethod
    def log_selection_batch(self, level=arg.DEFAULT, reset_after: bool = True) -> NoReturn:
        pass

    @abstractmethod
    def is_started(self) -> bool:
        pass

    @abstractmethod
    def is_finished(self) -> bool:
        pass

    @abstractmethod
    def get_percent(self, round_digits: int = 1, default_value: str = 'UNK') -> str:
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
    def update(self, position: int, step: Optional[int] = None, message: Optional[str] = None) -> NoReturn:
        pass

    @abstractmethod
    def start(self, position: int = 0) -> NoReturn:
        pass

    @abstractmethod
    def finish(self, position: Optional[int] = None, log_selection_batch: bool = True) -> NoReturn:
        pass

    @abstractmethod
    def iterate(
            self,
            items: Iterable,
            name: Optional[str] = None,
            expected_count: Optional[int] = None,
            step: Union[int, arg.DefaultArgument] = arg.DEFAULT,
            log_selection_batch: bool = True
    ) -> Iterable:
        pass
