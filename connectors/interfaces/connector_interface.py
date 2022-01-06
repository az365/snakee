from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.sourced_interface import SourcedInterface
    from loggers.logger_interface import LoggerInterface, LoggingLevel
    from loggers.extended_logger_interface import ExtendedLoggerInterface
    from loggers.progress_interface import ProgressInterface
    from connectors.conn_type import ConnType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.sourced_interface import SourcedInterface
    from ...loggers.logger_interface import LoggerInterface, LoggingLevel
    from ...loggers.extended_logger_interface import ExtendedLoggerInterface
    from ...loggers.progress_interface import ProgressInterface
    from ..conn_type import ConnType

Logger = Union[LoggerInterface, ExtendedLoggerInterface]
OptionalParent = Optional[SourcedInterface]

AUTO = arg.AUTO
DEFAULT_PATH_DELIMITER = '/'
CHUNK_SIZE = 8192


class ConnectorInterface(SourcedInterface, ABC):
    @abstractmethod
    def get_conn_type(self) -> ConnType:
        """Returns type of connector
        as ConnType enum-object with one of possible values:

        PostgresDatabase, ClickhouseDatabase, Table,
        LocalStorage, LocalFolder, FileMask, PartitionedLocalFile, LocalFile,
        S3Storage, S3Bucket, S3Folder, S3Object, TwinSync,
        etc.
        """
        pass

    @abstractmethod
    def get_config_dict(self) -> dict:
        """Returns dict with connector setting for save into json-config file or object."""
        pass

    @abstractmethod
    def get_path(self) -> str:
        """Returns full path to file/object as string, including prefix."""
        pass

    @abstractmethod
    def get_path_as_list(self) -> list:
        """Returns bread crumbs of path to file/object as list."""
        pass

    @abstractmethod
    def get_path_prefix(self) -> str:
        """Returns path prefix string for parent database, storage or filesystem,
        i.e. s3u:// for s3 storage or ~/ for local job folder.
        """
        pass

    @abstractmethod
    def get_path_delimiter(self) -> str:
        """Returns path delimiter symbols for parent database, storage or filesystem,
        i.e. / for Linux or s3, \\ for Windows, . for SQL schemeS and tables.
        """
        pass

    @abstractmethod
    def get_parent(self) -> OptionalParent:
        """Returns parent connector (i.e. folder for file) if provided."""
        pass

    @abstractmethod
    def set_parent(self, parent: SourcedInterface, reset: bool = False, inplace: bool = True):
        """Remember provided connector (or context) object as parent of this connector
        and register this connector as child of provided parend after initialization.
        """
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        """Returns child connectors (for HierarchicConnector) or data items (for LeafConnector instances)"""
        pass

    @abstractmethod
    def get_children(self) -> dict:
        """Returns dict with names and connectors of all child connectors"""
        pass

    @abstractmethod
    def get_child(self, name: str) -> SourcedInterface:
        """Find children connector object by name."""
        pass

    @abstractmethod
    def add_child(self, child: SourcedInterface):
        """Remember new child connector (i.e. register new file in folder)."""
        pass

    @abstractmethod
    def forget_child(self, child_or_name: Union[SourcedInterface, str], skip_errors: bool = False):
        """Closes related connection(s) and lets SnakeeContext forget the link to this connector."""
        pass

    @abstractmethod
    def get_storage(self) -> OptionalParent:
        """Returns parent storage of local file or s3 object."""
        pass

    @abstractmethod
    def get_context(self):
        """Returns SnakeeContext if available."""
        pass

    @abstractmethod
    def set_context(self, context, reset: bool = False):
        """Using for provide context object to connector after initialization."""
        pass

    @staticmethod
    @abstractmethod
    def is_context() -> bool:
        """Check is this object is SnakeeContext singleton."""
        pass

    @staticmethod
    def is_connector() -> bool:
        """Check is this object the child of AbstractConnector.
        Can be used instead of isinstance(obj, AbstractConnector).
        """
        return True

    @staticmethod
    @abstractmethod
    def is_storage() -> bool:
        """Check is this connector the child of AbstractStorage.
        Can be used instead of isinstance(obj, AbstractStorage).
        """
        pass

    @staticmethod
    @abstractmethod
    def is_folder() -> bool:
        """Check is this connector the child of AbstractFolder.
        Can be used instead of isinstance(obj, AbstractFolder).
        """
        pass

    @staticmethod
    @abstractmethod
    def has_hierarchy() -> bool:
        """Check is this connector the child of HierarchicConnector.
        Can be used instead of isinstance(obj, HierarchicConnector).
        """
        pass

    @abstractmethod
    def is_leaf(self) -> bool:
        """Check is this connector the child of LeafConnector.
        Can be used instead of isinstance(obj, LeafConnector).
        """
        pass

    @abstractmethod
    def is_root(self) -> bool:
        """Check is this object is the root of current connectors hierarchy,
        it means that this root node has no parent in tree graph.
        """
        pass

    @abstractmethod
    def is_existing(self) -> Optional[bool]:
        """Check is this object existing in storage.

        :return: bool (if checked) or None (if not appropriate)
        """

    @abstractmethod
    def is_verbose(self) -> bool:
        """Get verbose mode setting:
        True value means that connector must log and show its actions,
        False value means that connector will be silent.
        """
        pass

    @abstractmethod
    def get_logger(self, skip_missing: bool = True, create_if_not_yet: bool = True) -> Logger:
        """Returns current common (default) logger.

        :param skip_missing: do not raise errors if common logger is not available or not set.
        :param create_if_not_yet: create new logger if current logger is not set.
        :return: ExtendedLogger from SnakeeContext if context is defined, otherwise FallbackLogger.
        """
        pass

    @abstractmethod
    def log(
            self,
            msg: str,
            level: Union[LoggingLevel, int, arg.Auto] = AUTO,
            end: Union[str, arg.Auto] = AUTO,
            verbose: bool = True,
    ) -> None:
        """Log message with using current common logger

        :param msg: any text message as str
        :param level: int LoggingLevel: 10 Debug, 20 Info, 30 Warning, 40 Error, 50 Critical
        :param end: ending symbol for STDOUT line, can be \n (write line permanently) or \r (temporary line to rewrite)
        :param verbose: flag to show message in STDOUT, otherwise write log line in logger file only.
        :return: nothing
        """
        pass

    @abstractmethod
    def get_new_progress(self, name, count: Optional[int] = None, context=AUTO) -> ProgressInterface:
        """Initialize new progress-bar using ExtendedLogger

        :param name: name of Progress object corresponding title message for progress-bar
        :param count: expected count of items for count percent of execution over iterator
        :param context: SnakeeContext object for obtain common logger and settings
        :return: Progress object receiving iterable object for measure and show progress of iteration
        """
        pass
