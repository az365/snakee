from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg

Context = Any
Stream = Any
OptionalFields = Optional[Union[Iterable, str]]


class DataInterface(ABC):
    @abstractmethod
    def get_data(self) -> Union[Iterable, Any]:
        pass

    @abstractmethod
    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        pass

    @abstractmethod
    def get_meta(self, ex: OptionalFields = None) -> dict:
        pass

    @abstractmethod
    def set_meta(self, **meta):
        pass
