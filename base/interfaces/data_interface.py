from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Count, OptionalFields
    from base.interfaces.base_interface import BaseInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Count, OptionalFields
    from .base_interface import BaseInterface

Data = Union[Iterable, Any]


class SimpleDataInterface(BaseInterface, ABC):
    @abstractmethod
    def get_name(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_caption(self) -> str:
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

    @abstractmethod
    def get_count_repr(self, default: str = '<iter>') -> str:
        pass

    @abstractmethod
    def describe(
            self,
            show_header: bool = True,
            count: Count = None,
            comment: Optional[str] = None,
            depth: int = 1,
            display=None,
            **kwargs
    ):
        pass
