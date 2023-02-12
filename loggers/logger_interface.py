from abc import ABC, abstractmethod
from enum import Enum
from typing import Union, Optional


class LoggingLevel(Enum):
    Debug = 10
    Info = 20
    Warning = 30
    Error = 40
    Critical = 50


class LoggerInterface(ABC):
    @abstractmethod
    def log(self, msg: str, level: Union[LoggingLevel, int], *args, **kwargs) -> None:
        pass

    @abstractmethod
    def debug(self, msg: str) -> None:
        pass

    @abstractmethod
    def info(self, msg: str) -> None:
        pass

    @abstractmethod
    def warning(self, msg: str, category: Optional[Warning] = None, stacklevel: int = 2) -> None:
        pass

    @abstractmethod
    def error(self, msg: str) -> None:
        pass

    @abstractmethod
    def critical(self, msg: str) -> None:
        pass


AutoLogger = Optional[LoggerInterface]  # deprecated
