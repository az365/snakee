from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Union, Iterable, Generator

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.tree_interface import TreeInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base.interfaces.tree_interface import TreeInterface
    from .extended_logger_interface import ExtendedLoggerInterface

Logger = Union[ExtendedLoggerInterface, arg.Auto]


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
    def get_selection_logger(self, name=arg.AUTO) -> Logger:
        pass

    @abstractmethod
    def log(
            self, msg: str,
            level: Union[int, arg.Auto] = arg.AUTO,
            end: Union[str, arg.Auto] = arg.AUTO,
            verbose: Union[bool, arg.Auto] = arg.AUTO,
    ) -> None:
        pass

    @abstractmethod
    def log_selection_batch(self, level=arg.AUTO, reset_after: bool = True) -> None:
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
    def update(self, position: int, step: Optional[int] = None, message: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def start(self, position: int = 0) -> None:
        pass

    @abstractmethod
    def finish(self, position: Optional[int] = None, log_selection_batch: bool = True) -> None:
        pass

    @abstractmethod
    def iterate(
            self,
            items: Iterable,
            name: Optional[str] = None,
            expected_count: Optional[int] = None,
            step: Union[int, arg.Auto] = arg.AUTO,
            log_selection_batch: bool = True
    ) -> Generator:
        pass
