from abc import ABC
from typing import Optional, Iterable

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from interfaces import Auto, Context, Connector, Class, Name
    from connectors.abstract.abstract_connector import AbstractConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import Auto, Context, Connector, Class, Name
    from .abstract_connector import AbstractConnector


class HierarchicConnector(AbstractConnector, ABC):
    def __init__(
            self,
            name: Name,
            parent: Connector = None,
            children: Optional[dict] = None,
            context: Context = None,
            verbose: Optional[bool] = None,
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

    @classmethod
    @deprecated_with_alternative('get_default_child_obj_class')
    def get_default_child_class(cls) -> Class:
        return cls.get_default_child_obj_class()

    @classmethod
    def get_default_child_obj_class(cls, skip_missing: bool = False) -> Class:
        if hasattr(cls, 'get_default_child_type'):
            child_class = cls.get_default_child_type().get_class(skip_missing=skip_missing)
        else:
            child_class = None
        if not Auto.is_defined(child_class):
            child_class = super().get_default_child_obj_class(skip_missing=skip_missing)
        return child_class

    def get_child_class_by_name(self, name: Name):
        return self.get_default_child_obj_class()

    def child(self, name: Name, parent_field: Name = 'parent', **kwargs) -> Connector:
        cur_child = self.get_child(name)
        if not cur_child:
            child_class = self.get_child_class_by_name(name)
            if parent_field not in kwargs:
                kwargs[parent_field] = self
            cur_child = child_class(name, **kwargs)
            self.add_child(cur_child)
        return cur_child
