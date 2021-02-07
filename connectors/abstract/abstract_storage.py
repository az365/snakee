from abc import abstractmethod

try:  # Assume we're a sub-module in a package.
    from connectors import connector_classes as ct
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import connector_classes as ct
    from ...utils import arguments as arg


class AbstractStorage(ct.HierarchicConnector):
    def __init__(
            self,
            name,
            context,
            verbose=True,
    ):
        super().__init__(
            name=name,
            parent=context,
        )
        self.verbose = verbose

    @staticmethod
    def is_root():
        return True

    @abstractmethod
    def get_default_child_class(self):
        pass

    def get_context(self):
        return self.parent

    def get_parent(self):
        return self.get_context()

    def get_storage(self):
        return self

    def get_path_prefix(self):
        return ''

    def get_path_delimiter(self):
        return DEFAULT_PATH_DELIMITER

    def get_path(self):
        return self.get_path_prefix()

    def get_path_as_list(self):
        return [self.get_path()]
