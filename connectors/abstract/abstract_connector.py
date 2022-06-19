from abc import ABC
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Connector, ConnectorInterface, ConnType,
        LoggerInterface, ExtendedLoggerInterface, LoggingLevel, Message,
        AUTO, Auto, AutoBool, AutoConnector, AutoContext,
    )
    from base.abstract.tree_item import TreeItem
    from loggers.logging_context_stub import LoggingContextStub
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Connector, ConnectorInterface, ConnType,
        LoggerInterface, ExtendedLoggerInterface, LoggingLevel, Message,
        AUTO, Auto, AutoBool, AutoConnector, AutoContext,
    )
    from ...base.abstract.tree_item import TreeItem
    from ...loggers.logging_context_stub import LoggingContextStub

Native = ConnectorInterface
Logger = Union[LoggerInterface, ExtendedLoggerInterface]

DEFAULT_PATH_DELIMITER = '/'
DEFAULT_VERBOSE = True


class AbstractConnector(TreeItem, ConnectorInterface, ABC):
    def __init__(
            self,
            name: Union[str, int],
            parent: Connector = None,
            children: Optional[dict] = None,
            context: AutoContext = AUTO,
            verbose: AutoBool = AUTO,
    ):
        self._verbose = DEFAULT_VERBOSE
        super().__init__(name=name, parent=parent, children=children)
        self.set_context(context)
        self.set_verbose(verbose)

    def is_defined(self) -> bool:
        return self.get_name() is not None

    def is_accessible(self, verbose: bool = False) -> bool:
        parent = self.get_parent()
        if hasattr(parent, 'is_accessible'):
            return parent.is_accessible(verbose=verbose)
        else:
            return True

    def get_conn_type(self) -> ConnType:
        conn_type = ConnType.detect(self)
        if isinstance(conn_type, ConnType):
            return conn_type

    def get_type(self) -> ConnType:
        return self.get_conn_type()

    def is_verbose(self) -> bool:
        return self._verbose

    def set_verbose(self, verbose: AutoBool = AUTO, parent: AutoConnector = AUTO) -> Native:
        if not Auto.is_defined(verbose):
            parent = Auto.delayed_acquire(parent, self.get_parent)
            if hasattr(parent, 'is_verbose'):
                verbose = parent.is_verbose()
            elif hasattr(parent, 'verbose'):
                verbose = parent.verbose
            else:
                verbose = DEFAULT_VERBOSE
        self._verbose = verbose
        return self

    def set_context(self, context: AutoContext, reset: bool = False, inplace: bool = True) -> Optional[Native]:
        if context:
            parent = self.get_parent()
            if Auto.is_defined(parent):
                parent.set_context(context, reset=False, inplace=True)
            elif Auto.is_defined(context):
                self.set_parent(context, reset=False, inplace=True)
        if not inplace:
            return self

    def get_storage(self) -> Connector:
        parent = self.get_parent()
        if parent:
            if hasattr(parent, 'is_storage'):
                if parent.is_storage():
                    return self._assume_connector(parent)
            if hasattr(parent, 'get_storage'):
                return parent.get_storage()

    def get_logger(self, force: bool = True) -> Logger:
        logger = super().get_logger(skip_missing=True)
        if logger:
            return logger
        elif force:
            return LoggingContextStub().get_logger(force)

    def log(
            self,
            msg: Message,
            level: Union[LoggingLevel, int, Auto] = AUTO,
            end: Union[str, Auto] = AUTO,
            truncate: bool = True,
            force: bool = False,
            verbose: bool = True,
            stacklevel: Optional[int] = 1,
            **kwargs
    ):
        logger = self.get_logger(force=force)
        if isinstance(logger, ExtendedLoggerInterface):
            if stacklevel is not None:
                stacklevel += 1
            logger.log(
                msg=msg, level=level, stacklevel=stacklevel,
                end=end, truncate=truncate, verbose=verbose,
                **kwargs,
            )
        elif logger:
            logger.log(msg=msg, level=level)
        return self

    def get_new_progress(self, name: str, count: Optional[int] = None, context: AutoContext = AUTO):
        logger = self.get_logger()
        if Auto.is_defined(context) and not Auto.is_defined(logger):
            logger = context.get_logger()
        if isinstance(logger, ExtendedLoggerInterface) or hasattr(logger, 'get_new_progress'):
            return logger.get_new_progress(name, count=count, context=context)

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

    def forget(self) -> None:
        if hasattr(self, 'close'):
            self.close()
        context = self.get_context()
        if context:
            context.forget_conn(self)

    def is_existing(self, verbose: AutoBool = AUTO) -> Optional[bool]:
        return None

    @staticmethod
    def _assume_connector(connector) -> ConnectorInterface:
        return connector
