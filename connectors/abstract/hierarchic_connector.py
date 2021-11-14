from abc import ABC, abstractmethod
from typing import Optional, Iterable

try:  # Assume we're a sub-module in a package.
    from interfaces import AUTO, Auto, AutoBool, AutoContext, Connector, Name
    from connectors.abstract.abstract_connector import AbstractConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import AUTO, Auto, AutoBool, AutoContext, Connector, Name
    from .abstract_connector import AbstractConnector


class HierarchicConnector(AbstractConnector, ABC):
    def __init__(
            self,
            name: Name,
            parent: Connector = None,
            children: Optional[dict] = None,
            context: AutoContext = AUTO,
            verbose: AutoBool = AUTO,
    ):
        super().__init__(
            name=name,
            parent=parent,
            children=children,
            context=context,
            verbose=verbose,
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

    def get_child_class_by_name(self, name: Name):
        return self.get_default_child_class()

    def child(self, name: Name, parent_field: Name = 'parent', **kwargs) -> Connector:
        cur_child = self.get_child(name)
        if not cur_child:
            child_class = self.get_child_class_by_name(name)
            if parent_field not in kwargs:
                kwargs[parent_field] = self
            cur_child = child_class(name, **kwargs)
            self.add_child(cur_child)
        return cur_child
