from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import AUTO, Auto, AutoBool, AutoContext
    from connectors.abstract.hierarchic_connector import HierarchicConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import AUTO, Auto, AutoBool, AutoContext
    from .hierarchic_connector import HierarchicConnector

Native = HierarchicConnector
AutoParent = Union[HierarchicConnector, arg.Auto]


class AbstractFolder(HierarchicConnector, ABC):
    def __init__(
            self,
            name: str,
            parent: HierarchicConnector,
            children: Optional[dict] = None,
            context: AutoContext = AUTO,
            verbose: AutoBool = arg.AUTO,
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
            verbose=arg.AUTO,
    ):
        super().__init__(
            name=name,
            parent=parent,
            verbose=verbose,
        )

    @abstractmethod
    def get_default_child_class(self) -> Callable:
        pass


class HierarchicFolder(AbstractFolder):
    def __init__(
            self,
            name: str,
            parent: HierarchicConnector,
            verbose: AutoBool = arg.AUTO,
    ):
        super().__init__(
            name=name,
            parent=parent,
            verbose=verbose,
        )

    def get_default_child_class(self) -> Callable:
        return self.__class__

    def get_folders(self) -> Iterable:
        for obj in self.get_items():
            if hasattr(obj, 'is_folder'):
                if obj.is_folder():  # isinstance(obj, (AbstractFolder, ct.AbstractFolder, ct.AbstractFile)):
                    yield obj

    def folder(self, name, **kwargs) -> AbstractFolder:
        return self.child(name, parent=self, **kwargs)
