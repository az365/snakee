from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Iterable, Generator, Union, Any
import logging

try:  # Assume we're a submodule in a package.
    from base.interfaces.base_interface import BaseInterface
    from base.interfaces.tree_interface import TreeInterface
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.interfaces.base_interface import BaseInterface
    from ..base.interfaces.tree_interface import TreeInterface
    from .logger_interface import LoggerInterface

BaseLogger = Union[LoggerInterface, Any]
Context = Optional[BaseInterface]
Name = str
Count = Optional[int]
Formatter = Union[str, logging.Formatter]
File = BaseInterface

DEFAULT_LOGGER_NAME = 'default'
DEFAULT_FORMATTER = '%(asctime)s - %(levelname)s - %(message)s'
DEFAULT_ENCODING = 'utf8'


class LoggingLevel(Enum):
    Debug = logging.DEBUG
    Info = logging.INFO
    Warning = logging.WARNING
    Error = logging.ERROR
    Critical = logging.CRITICAL

    def get_value(self) -> int:
        return self.value

    def get_name(self) -> str:
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

    def get_method_name(self) -> str:
        return self.get_name()

    @classmethod
    def get_default(cls):
        return cls.Warning


Level = Union[LoggingLevel, int, None]

DEFAULT_LOGGING_LEVEL = LoggingLevel.get_default()


class ExtendedLoggerInterface(TreeInterface, LoggerInterface, ABC):
    @staticmethod
    @abstractmethod
    def is_common_logger() -> bool:
        pass

    @abstractmethod
    def get_base_logger(self) -> BaseLogger:
        pass

    @abstractmethod
    def get_level(self) -> LoggingLevel:
        pass

    @abstractmethod
    def get_handlers(self) -> list:
        pass

    @abstractmethod
    def add_handler(self, handler: logging.Handler, if_not_added: bool = True) -> LoggerInterface:
        if handler not in self.get_handlers() or not if_not_added:
            self.get_base_logger().addHandler(handler)
        return self

    @abstractmethod
    def set_file(
            self, file: Union[File, Name],
            encoding: str = DEFAULT_ENCODING,
            level: Level = DEFAULT_LOGGING_LEVEL,
            formatter: Formatter = DEFAULT_FORMATTER,
            if_not_added: bool = True,
    ) -> LoggerInterface:
        pass

    @abstractmethod
    def progress(
            self,
            items: Iterable,
            name: Name = 'Progress',
            count: Count = None,
            step: Count = None,
            context: Context = None,
    ) -> Generator:
        pass

    @abstractmethod
    def get_new_progress(self, name: Name, count: Count = None, context: Context = None):
        pass

    @abstractmethod
    def get_selection_logger(self, name: Optional[Name] = None, **kwargs):
        pass

    @abstractmethod
    def set_selection_logger(self, selection_logger, skip_errors: bool = True) -> None:
        pass

    @abstractmethod
    def log(
            self,
            msg: Union[str, list, tuple],
            level: Level = None,
            logger: Optional[BaseLogger] = None,
            end: Optional[str] = None,
            verbose: bool = True,
            truncate: bool = True,
            **kwargs
    ) -> None:
        pass
