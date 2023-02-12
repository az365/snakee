from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Generator
from datetime import timedelta, datetime

try:  # Assume we're a submodule in a package.
    from base.classes.enum import DynamicEnum
    from base.interfaces.tree_interface import TreeInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.classes.enum import DynamicEnum
    from ..base.interfaces.tree_interface import TreeInterface
    from .extended_logger_interface import ExtendedLoggerInterface

Native = Union[TreeInterface, ExtendedLoggerInterface]
Logger = Optional[ExtendedLoggerInterface]


class OperationStatus(DynamicEnum):
    New = 'new'
    InProgress = 'in_progress'
    Done = 'done'


OperationStatus.prepare()


class ProgressInterface(TreeInterface, ABC):
    @abstractmethod
    def get_state(self) -> OperationStatus:
        pass

    @abstractmethod
    def set_state(self, state: OperationStatus):
        pass

    @abstractmethod
    def get_expected_count(self) -> Optional[int]:
        pass

    @abstractmethod
    def set_expected_count(self, count: int):
        pass

    @abstractmethod
    def get_start_time(self) -> Optional[datetime]:
        pass

    @abstractmethod
    def set_start_time(self, start_time: datetime):
        pass

    @abstractmethod
    def get_past_time(self) -> Optional[timedelta]:
        pass

    @abstractmethod
    def set_past_time(self, past_time: timedelta):
        pass

    @abstractmethod
    def get_logger(self) -> Logger:
        pass

    @abstractmethod
    def get_position(self) -> int:
        pass

    @abstractmethod
    def set_position(self, position: int):
        pass

    @abstractmethod
    def get_selection_logger(self, name: Optional[str] = None) -> Logger:
        pass

    @abstractmethod
    def log(
            self,
            msg: str,
            level: Optional[int] = None,
            end: Optional[str] = None,
            verbose: Optional[bool] = None,
    ) -> None:
        pass

    @abstractmethod
    def log_selection_batch(self, level: Optional[int] = None, reset_after: bool = True) -> None:
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
            step: Optional[int] = None,
            log_selection_batch: bool = True
    ) -> Generator:
        pass
