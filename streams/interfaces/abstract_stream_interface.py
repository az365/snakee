from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.sourced_interface import SourcedInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.sourced_interface import SourcedInterface

Data = Union[SourcedInterface, Any]
OptionalFields = Optional[Union[Iterable, str]]


class StreamInterface(SourcedInterface, ABC):
    @classmethod
    @abstractmethod
    def get_stream_type(cls):
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def map(self, function: Callable) -> Data:
        pass

    @abstractmethod
    def filter(self, function: Callable) -> Data:
        pass

    @abstractmethod
    def take(self, count: int) -> Data:
        pass

    @abstractmethod
    def get_source(self):
        pass

    @abstractmethod
    def get_logger(self):
        pass

    @abstractmethod
    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True, truncate=True, force=True):
        pass

    @abstractmethod
    def get_data(self) -> Data:
        pass

    @abstractmethod
    def set_data(self, data: Data, inplace: bool):
        pass

    @abstractmethod
    def apply_to_data(self, function: Callable, *args, dynamic=False, **kwargs):
        pass

    @abstractmethod
    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        pass
