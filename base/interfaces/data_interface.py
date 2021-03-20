from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.sourced_interface import SourcedInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from utils import arguments as arg
    from ..interfaces.sourced_interface import SourcedInterface

Data = Union[Iterable, Any]
OptionalFields = Optional[Union[Iterable, str]]


class DataInterface(SourcedInterface, ABC):
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
