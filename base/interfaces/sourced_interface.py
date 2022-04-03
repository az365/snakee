from abc import ABC, abstractmethod
from typing import Optional, NoReturn

try:  # Assume we're a submodule in a package.
    from base.interfaces.base_interface import BaseInterface
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...loggers.logger_interface import LoggerInterface
    from ..interfaces.base_interface import BaseInterface

Source = BaseInterface
Logger = Optional[LoggerInterface]


class SourcedInterface(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def set_name(self, name: str, inplace=True) -> Optional[Source]:
        pass

    @abstractmethod
    def get_source(self) -> Source:
        pass

    @abstractmethod
    def set_source(self, source: Source, reset: bool = True) -> Optional[Source]:
        pass

    @abstractmethod
    def register(self, check: bool = True) -> NoReturn:
        pass

    @abstractmethod
    def get_logger(self) -> Logger:
        pass
