from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers import logger_classes
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...loggers import logger_classes


AUTO = arg.DEFAULT
DEFAULT_PATH_DELIMITER = '/'
CHUNK_SIZE = 8192


class AbstractConnector(ABC):
    def __init__(
            self,
            name,
            parent=None,
    ):
        self.name = name
        self.parent = parent

    @staticmethod
    def is_context():
        return False

    def is_root(self):
        return self.get_parent().is_context()

    @abstractmethod
    def has_hierarchy(self):
        pass

    def get_name(self):
        return self.name

    def get_parent(self):
        return self.parent

    def get_storage(self):
        parent = self.get_parent()
        if parent and not self.is_root():
            return parent.get_storage()

    def get_context(self):
        parent = self.get_parent()
        if parent:
            return self.get_parent().get_context()

    def get_logger(self):
        if self.get_context():
            return self.get_context().get_logger()
        else:
            return logger_classes.get_logger()

    def log(self, msg, level=AUTO, end=AUTO, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def get_names_hierarchy(self):
        if self.is_root():
            hierarchy = list()
        else:
            hierarchy = self.get_parent().get_names_hierarchy()
        return hierarchy + [self.get_name()]

    def get_path_prefix(self):
        return self.get_storage().get_path_prefix()

    def get_path_delimiter(self):
        storage = self.get_storage()
        if storage:
            return storage.get_path_delimiter()
        else:
            return DEFAULT_PATH_DELIMITER

    def get_path(self):
        if self.is_root():
            return self.get_path_prefix()
        else:
            return self.get_parent().get_path() + self.get_path_delimiter() + self.get_name()

    def get_path_as_list(self):
        if self.is_root():
            return [self.get_path_prefix()]
        else:
            return self.get_parent().get_path_as_list() + self.get_name().split(self.get_path_delimiter())

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('parent')
        return meta

    def get_config(self):
        data = self.__dict__.copy()
        for k, v in data.items():
            if k == 'parent':
                if hasattr(v, 'name'):
                    data[k] = v.get_name()
        return data
