from abc import abstractmethod

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.abstract.hierarchic_connector import HierarchicConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .hierarchic_connector import HierarchicConnector


class AbstractFolder(HierarchicConnector):
    def __init__(
            self,
            name,
            parent,
            verbose=arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        self.verbose = verbose if verbose is not None and verbose != arg.DEFAULT else parent.verbose

    @staticmethod
    def is_root():
        return False

    @staticmethod
    def is_storage():
        return False

    @staticmethod
    def is_folder():
        return True

    @abstractmethod
    def get_default_child_class(self):
        pass


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
            name,
            parent,
            verbose=arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        self.verbose = verbose if verbose is not None and verbose != arg.DEFAULT else parent.verbose

    def get_default_child_class(self):
        return self.__class__

    def get_folders(self):
        for obj in self.get_items():
            if hasattr(obj, 'is_folder'):
                if obj.is_folder():  # isinstance(obj, (AbstractFolder, ct.AbstractFolder, ct.AbstractFile)):
                    yield obj
