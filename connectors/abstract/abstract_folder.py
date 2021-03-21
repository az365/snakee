from abc import ABC, abstractmethod
from typing import Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.abstract.hierarchic_connector import HierarchicConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .hierarchic_connector import HierarchicConnector


class AbstractFolder(HierarchicConnector, ABC):
    def __init__(
            self,
            name: str,
            parent: HierarchicConnector,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        if hasattr(parent, 'verbose') and not arg.is_defined(verbose):
            verbose = parent.verbose
        self.verbose = verbose

    def is_root(self):
        return False

    @staticmethod
    def is_storage():
        return False

    @staticmethod
    def is_folder():
        return True


class FlatFolder(AbstractFolder):
    def __init__(
            self,
            name,
            parent,
            verbose=arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            parent=parent,
            verbose=verbose,
        )

    @abstractmethod
    def get_default_child_class(self):
        pass


class HierarchicFolder(AbstractFolder, HierarchicConnector):
    def __init__(
            self,
            name: str,
            parent: HierarchicConnector,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        if hasattr(parent, 'verbose') and not arg.is_defined(verbose):
            verbose = parent.verbose
        self.verbose = verbose

    def get_default_child_class(self):
        return self.__class__

    def get_folders(self):
        for obj in self.get_items():
            if hasattr(obj, 'is_folder'):
                if obj.is_folder():  # isinstance(obj, (AbstractFolder, ct.AbstractFolder, ct.AbstractFile)):
                    yield obj

    def folder(self, name, **kwargs):
        return self.child(name, parent=self, **kwargs)
