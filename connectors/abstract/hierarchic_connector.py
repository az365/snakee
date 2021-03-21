from abc import ABC, abstractmethod
from typing import Iterable

try:  # Assume we're a sub-module in a package.
    from connectors.abstract import abstract_connector as ac
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..abstract import abstract_connector as ac


class HierarchicConnector(ac.AbstractConnector, ABC):
    def __init__(
            self,
            name,
            parent=None,
            children=None,
    ):
        super().__init__(
            name=name,
            parent=parent,
            children=children,
        )

    @staticmethod
    def has_hierarchy() -> bool:
        return True

    def is_leaf(self) -> bool:
        return False

    def get_leafs(self) -> Iterable:
        for child in self.get_children():
            if hasattr(child, 'is_leaf'):
                if child.is_leaf():
                    yield child
            if hasattr(child, 'get_leafs'):
                yield from self.get_leafs()

    @staticmethod
    @abstractmethod
    def get_default_child_class():
        pass

    def get_child_class_by_name(self, name):
        return self.get_default_child_class()

    def child(self, name, **kwargs):
        cur_child = self.get_child(name)
        if not cur_child:
            child_class = self.get_child_class_by_name(name)
            if 'parent' not in kwargs:
                kwargs['parent'] = self
            cur_child = child_class(name, **kwargs)
            self.add_child(cur_child)
        return cur_child
