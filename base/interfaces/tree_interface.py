from abc import ABC, abstractmethod
from typing import Optional, Iterable, Sequence, Union, Any

try:  # Assume we're a submodule in a package.
    from content.items.simple_items import Class
    from base.interfaces.data_interface import ContextualDataInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...content.items.simple_items import Class
    from ..interfaces.data_interface import ContextualDataInterface

Parent = Optional[Union[ContextualDataInterface, Any]]
Child = Optional[ContextualDataInterface]
OptionalFields = Optional[Union[str, Iterable]]

META_MEMBER_MAPPING = dict(_data='children', _source='parent')


class TreeInterface(ContextualDataInterface, ABC):
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
    def add_child(self, child: ContextualDataInterface):
        pass

    @abstractmethod
    def forget_child(self, child_or_name: Union[ContextualDataInterface, str], skip_errors=False):
        pass

    @abstractmethod
    def close(self) -> int:
        """Close connection(s) or fileholder(s) if it's opened.

        :return: count of closed connections.
        """
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

    @classmethod
    @abstractmethod
    def set_parent_obj_classes(cls, classes: Sequence):
        pass

    @classmethod
    @abstractmethod
    def set_child_obj_classes(cls, classes: Sequence):
        pass

    @classmethod
    @abstractmethod
    def get_parent_obj_classes(cls) -> Sequence:
        pass

    @classmethod
    @abstractmethod
    def get_child_obj_classes(cls) -> Sequence:
        pass

    @classmethod
    @abstractmethod
    def get_default_parent_obj_class(cls) -> Optional[Class]:
        pass

    @classmethod
    @abstractmethod
    def get_default_child_obj_class(cls) -> Optional[Class]:
        pass
