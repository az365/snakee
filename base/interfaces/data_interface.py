from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto
    from base.interfaces.base_interface import BaseInterface
    from base.interfaces.line_output_interface import LineOutputInterface
    from base.interfaces.contextual_interface import ContextualInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import Auto
    from .base_interface import BaseInterface
    from .line_output_interface import LineOutputInterface
    from .contextual_interface import ContextualInterface

Data = Union[Iterable, Any]
OptionalFields = Union[Iterable, str, None]


class SimpleDataInterface(BaseInterface, LineOutputInterface, ABC):
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
    def apply_to_data(self, function: Callable, *args, dynamic: bool = False, **kwargs):
        pass

    @abstractmethod
    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        pass


class ContextualDataInterface(ContextualInterface, SimpleDataInterface, ABC):
    @abstractmethod
    def get_str_count(self, default: str = '<iter>') -> str:
        pass

    @abstractmethod
    def get_count_repr(self, default: str = '<iter>') -> str:
        pass
