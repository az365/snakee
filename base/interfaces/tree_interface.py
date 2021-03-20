from abc import ABC, abstractmethod
from typing import Union, Optional, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract.named import AbstractNamed
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from utils import arguments as arg
    from base.abstract.named import AbstractNamed

Parent = Optional[Union[AbstractNamed, Any]]
OptionalFields = Optional[Union[str, Iterable]]

META_MEMBER_MAPPING = dict(_data='children', _source='parent')


class TreeInterface(ABC):
    @abstractmethod
    def get_name(self):
        pass

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
    def get_child(self, name: str) -> Optional[AbstractNamed]:
        pass

    @abstractmethod
    def add_child(self, child: AbstractNamed):
        pass

    @abstractmethod
    def forget_child(self, child_or_name: Union[AbstractNamed, str], skip_errors=False):
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
