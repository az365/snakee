from abc import abstractmethod

try:  # Assume we're a sub-module in a package.
    from connectors import connector_classes as ct
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import connector_classes as ct
    from ...utils import arguments as arg


class AbstractFolder(ct.HierarchicConnector):
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


class HierarchicFolder(ct.HierarchicConnector):
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
            if isinstance(obj, (AbstractFolder, ct.AbstractFolder, ct.AbstractFile)):
                yield obj
