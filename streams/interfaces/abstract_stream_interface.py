from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.data_interface import DataInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from base.interfaces.data_interface import DataInterface

Context = Any
Stream = Any
OptionalFields = Optional[Union[Iterable, str]]


class StreamInterface(DataInterface, ABC):
    @classmethod
    @abstractmethod
    def get_stream_type(cls):
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def map(self, function: Callable) -> Stream:
        pass

    @abstractmethod
    def filter(self, function: Callable) -> Stream:
        pass

    @abstractmethod
    def take(self, count: int) -> Stream:
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
