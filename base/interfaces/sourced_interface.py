from abc import ABC, abstractmethod
from typing import Optional

try:  # Assume we're a submodule in a package.
    from loggers.logger_interface import LoggerInterface
    from base.interfaces.base_interface import BaseInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...loggers.logger_interface import LoggerInterface
    from ..interfaces.base_interface import BaseInterface

Source = Optional[BaseInterface]

COLS_FOR_META = ('defined', 3), ('key', 20), ('value', 30), ('actual_type', 14), ('expected_type', 20), ('default', 20)
COLS_FOR_DICT = [('key', 20), 'value']


# @deprecated
class SourcedInterface(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def set_name(self, name: str, inplace=True) -> Source:
        pass

    @abstractmethod
    def get_source(self) -> BaseInterface:
        pass

    @abstractmethod
    def set_source(self, source: Source, reset: bool = True) -> Source:
        pass

    @abstractmethod
    def register_in_source(self, check: bool = True) -> BaseInterface:
        pass

    @abstractmethod
    def get_logger(self) -> Optional[LoggerInterface]:
        pass
