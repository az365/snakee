from abc import ABC
from typing import Optional, Union, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import Connector, ConnectorInterface, LoggerInterface, ExtendedLoggerInterface, AutoContext
    from base.abstract.tree_item import TreeItem
    from loggers.fallback_logger import FallbackLogger
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import Connector, ConnectorInterface, LoggerInterface, ExtendedLoggerInterface, AutoContext
    from ...base.abstract.tree_item import TreeItem
    from ...loggers.fallback_logger import FallbackLogger

Logger = Union[LoggerInterface, ExtendedLoggerInterface]

DEFAULT_PATH_DELIMITER = '/'
CHUNK_SIZE = 8192


class AbstractConnector(TreeItem, ConnectorInterface, ABC):
    def __init__(
            self,
            name: Union[str, int],
            parent: Connector = None,
            children: Optional[dict] = None,
    ):
        super().__init__(name=name, parent=parent, children=children)

    def get_storage(self) -> Connector:
        parent = self.get_parent()
        if parent:
            if hasattr(parent, 'is_storage'):
                if parent.is_storage():
                    return self._assume_connector(parent)
            if hasattr(parent, 'get_storage'):
                return parent.get_storage()

    def get_logger(self, skip_missing: bool = True, create_if_not_yet: bool = True) -> Logger:
        logger = super().get_logger(skip_missing=skip_missing)
        if logger:
            return logger
        elif create_if_not_yet:
            return FallbackLogger()

    def log(self, msg: str, level=arg.AUTO, end: Union[str, arg.Auto] = arg.AUTO, verbose: bool = True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def get_new_progress(self, name: str, count: Optional[int] = None, context: AutoContext = arg.AUTO):
        logger = self.get_logger()
        if hasattr(logger, 'get_new_progress'):
            return logger.get_new_progress(name, count=count, context=context)

    @staticmethod
    def _assume_connector(obj) -> ConnectorInterface:
        return obj

    def get_path_prefix(self) -> str:
        return self.get_storage().get_path_prefix()

    def get_path_delimiter(self) -> str:
        storage = self.get_storage()
        if hasattr(storage, 'get_path_delimiter'):
            return storage.get_path_delimiter()
        else:
            return DEFAULT_PATH_DELIMITER

    def get_path(self) -> str:
        if self.is_root():
            return self.get_path_prefix()
        else:
            parent_path = self.get_parent().get_path()
            if parent_path:
                return parent_path + self.get_path_delimiter() + self.get_name()
            else:
                return self.get_name()

    def get_path_as_list(self) -> list:
        if self.is_root():
            return [self.get_path_prefix()]
        else:
            return self.get_parent().get_path_as_list() + self.get_name().split(self.get_path_delimiter())

    def get_config_dict(self) -> dict:
        config = self.__dict__.copy()
        for k, v in config.items():
            k = self._get_meta_field_by_member_name(k)
            if k in ('parent', 'context') and hasattr(v, 'name'):
                v = v.get_name()
            elif hasattr(v, 'get_config_dict'):
                v = v.get_config_dict()
            else:
                v = None
            config[k] = v
        return config

    def forget(self) -> NoReturn:
        if hasattr(self, 'close'):
            self.close()
        context = self.get_context()
        if context:
            context.forget_conn(self)
