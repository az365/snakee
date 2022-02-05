from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, Any

try:  # Assume we're a submodule in a package.
    from base.interfaces.base_interface import BaseInterface
    from base.interfaces.contextual_interface import ContextualInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .base_interface import BaseInterface
    from .contextual_interface import ContextualInterface

Data = Union[Iterable, Any]
OptionalFields = Optional[Union[Iterable, str]]


class SimpleDataInterface(BaseInterface, ABC):
    @abstractmethod
    def get_name(self) -> Optional[str]:
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


class ContextualDataInterface(ContextualInterface, SimpleDataInterface, ABC):
    pass
