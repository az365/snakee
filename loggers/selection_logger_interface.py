from abc import ABC, abstractmethod
from typing import Optional, Iterable

try:  # Assume we're a submodule in a package.
    from loggers.extended_logger_interface import ExtendedLoggerInterface
    from loggers.detailed_message import DetailedMessage, SelectionError
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .extended_logger_interface import ExtendedLoggerInterface
    from .detailed_message import DetailedMessage, SelectionError

SELECTION_LOGGER_NAME = 'SelectorMessageCollector'


class SelectionLoggerInterface(ExtendedLoggerInterface, ABC):
    @abstractmethod
    def get_keys(self, ordered: bool = True) -> Iterable:
        pass

    @abstractmethod
    def get_key_count(self) -> int:
        pass

    @abstractmethod
    def get_count(self, key: str) -> Optional[int]:
        pass

    @abstractmethod
    def get_counts(self) -> dict:
        pass

    @abstractmethod
    def get_examples(self, key: str) -> list:
        pass

    @abstractmethod
    def get_example_count(self, key: str) -> int:
        pass

    @abstractmethod
    def add_message(self, message: DetailedMessage, key_as_str: bool = True) -> None:
        pass

    @abstractmethod
    def get_ok_key(self) -> str:
        pass

    @abstractmethod
    def get_err_keys(self) -> list:
        pass

    @abstractmethod
    def get_unordered_keys(self) -> Iterable:
        pass

    @abstractmethod
    def get_ordered_keys(self) -> list:
        pass

    @abstractmethod
    def is_new_key(self, message: SelectionError, key_as_str: bool = True) -> bool:
        pass

    @abstractmethod
    def get_message_batch(self, as_str: bool = True) -> Iterable:
        pass

    @abstractmethod
    def log_selection_error(self, func, in_fields: list, in_values: list, in_record: dict, message: str) -> None:
        pass

    @abstractmethod
    def show_error(self, message: SelectionError) -> None:
        pass

    @abstractmethod
    def has_errors(self) -> bool:
        pass

    @abstractmethod
    def get_err_count(self) -> int:
        pass
