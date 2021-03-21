from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.abstract.hierarchic_connector import HierarchicConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .hierarchic_connector import HierarchicConnector


DEFAULT_PATH_DELIMITER = '/'


class AbstractStorage(HierarchicConnector, ABC):
    def __init__(self, name, context, verbose=True):
        super().__init__(name=name, parent=context)
        self.verbose = verbose

    def is_root(self):
        return True

    @staticmethod
    def is_storage():
        return True

    @staticmethod
    def is_folder():
        return False

    @staticmethod
    @abstractmethod
    def get_default_child_class():
        pass

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
