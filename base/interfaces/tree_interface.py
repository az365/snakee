from abc import ABC, abstractmethod
from typing import Union, Optional, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.data_interface import DataInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..interfaces.data_interface import DataInterface

Parent = Optional[Union[DataInterface, Any]]
Child = Optional[DataInterface]
OptionalFields = Optional[Union[str, Iterable]]

META_MEMBER_MAPPING = dict(_data='children', _source='parent')


class TreeInterface(DataInterface, ABC):
    @abstractmethod
    def get_parent(self) -> Parent:
        pass

    @abstractmethod
    def set_parent(self, parent: Parent, reset: bool = False, inplace: bool = True):
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def get_children(self) -> dict:
        pass

    @abstractmethod
    def get_child(self, name: str) -> Child:
        pass

    @abstractmethod
    def add_child(self, child: DataInterface):
        pass

    @abstractmethod
    def forget_child(self, child_or_name: Union[DataInterface, str], skip_errors=False):
        pass

    @staticmethod
    def has_hierarchy():
        return True

    @abstractmethod
    def is_leaf(self) -> bool:
        pass

    @abstractmethod
    def is_root(self) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def is_context() -> bool:
        pass

    @abstractmethod
    def get_context(self):
        pass

    @abstractmethod
    def set_context(self, context, reset=False):
        pass
