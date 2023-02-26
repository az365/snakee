from abc import ABC
from typing import Optional, Callable, Iterable

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from interfaces import Context, Class
    from connectors.abstract.hierarchic_connector import HierarchicConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import Context, Class
    from .hierarchic_connector import HierarchicConnector

Native = HierarchicConnector


class AbstractFolder(HierarchicConnector, ABC):
    def __init__(
            self,
            name: str,
            parent: HierarchicConnector,
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

    def is_root(self) -> bool:
        return False

    @staticmethod
    def is_storage() -> bool:
        return False

    @staticmethod
    def is_folder() -> bool:
        return True


class FlatFolder(AbstractFolder):
    def __init__(
            self,
            name,
            parent,
            verbose=None,
    ):
        super().__init__(name=name, parent=parent, verbose=verbose)


class HierarchicFolder(AbstractFolder):
    def __init__(
            self,
            name: str,
            parent: HierarchicConnector,
            verbose: Optional[bool] = None,
    ):
        super().__init__(name=name, parent=parent, verbose=verbose)

    @classmethod
    @deprecated_with_alternative('get_default_child_obj_class()')
    def get_default_child_class(cls) -> Callable:
        return cls.get_default_child_obj_class()

    @classmethod
    def get_default_child_obj_class(cls, skip_missing: bool = False) -> Class:
        child_class = super().get_default_child_obj_class()
        if not child_class:
            child_class = cls
        return child_class

    def get_folders(self) -> Iterable:
        for obj in self.get_items():
            if hasattr(obj, 'is_folder'):
                if obj.is_folder():  # isinstance(obj, (AbstractFolder, ct.AbstractFolder, ct.AbstractFile)):
                    yield obj

    def folder(self, name, **kwargs) -> AbstractFolder:
        child = self.child(name, parent=self, **kwargs)
        return self._assume_native(child)

    @staticmethod
    def _assume_native(obj) -> AbstractFolder:
        return obj
