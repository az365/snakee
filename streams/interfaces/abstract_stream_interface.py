from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.sourced_interface import SourcedInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.sourced_interface import SourcedInterface

Stream = SourcedInterface
Data = Union[Stream, Any]
OptionalFields = Optional[Union[Iterable, str]]


class StreamInterface(SourcedInterface, ABC):
    @classmethod
    @abstractmethod
    def get_stream_type(cls):
        pass

    @abstractmethod
    def set_name(self, name: str, register: bool = True, inplace: bool = False) -> Optional[Stream]:
        pass

    @abstractmethod
    def get_count(self) -> Optional[int]:
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
    def skip(self, count: int) -> Stream:
        pass

    @abstractmethod
    def get_source(self) -> SourcedInterface:
        pass

    @abstractmethod
    def get_logger(self) -> SourcedInterface:
        pass

    @abstractmethod
    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True, truncate=True, force=True):
        pass

    @abstractmethod
    def get_data(self) -> Data:
        pass

    @abstractmethod
    def set_data(self, data: Data, inplace: bool) -> Optional[Stream]:
        pass

    @abstractmethod
    def apply_to_data(self, function: Callable, dynamic: bool = False, *args, **kwargs) -> Stream:
        pass

    @abstractmethod
    def get_calc(self, function: Callable, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        pass

    @abstractmethod
    def stream(self, data: Data, **kwargs) -> Stream:
        pass

    @abstractmethod
    def to_stream(self) -> Stream:
        pass

    @abstractmethod
    def forget(self) -> NoReturn:
        pass

    @abstractmethod
    def get_links(self) -> Iterable:
        pass
