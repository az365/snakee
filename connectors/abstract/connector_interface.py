from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.sourced_interface import SourcedInterface
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface
    from loggers.progress_interface import ProgressInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.sourced_interface import SourcedInterface
    from ...loggers.logger_interface import LoggerInterface
    from ...loggers.extended_logger_interface import ExtendedLoggerInterface
    from ...loggers.progress_interface import ProgressInterface

Logger = Union[LoggerInterface, ExtendedLoggerInterface]
OptionalParent = Optional[SourcedInterface]

AUTO = arg.DEFAULT
DEFAULT_PATH_DELIMITER = '/'
CHUNK_SIZE = 8192


class ConnectorInterface(SourcedInterface, ABC):
    @staticmethod
    @abstractmethod
    def is_storage() -> bool:
        pass

    @staticmethod
    @abstractmethod
    def is_folder() -> bool:
        pass

    @staticmethod
    @abstractmethod
    def has_hierarchy() -> bool:
        pass

    @abstractmethod
    def get_storage(self) -> OptionalParent:
        pass

    @abstractmethod
    def get_logger(self, skip_missing=True, create_if_not_yet=True) -> Logger:
        pass

    @abstractmethod
    def log(self, msg, level=AUTO, end=AUTO, verbose=True) -> NoReturn:
        pass

    @abstractmethod
    def get_new_progress(self, name, count=None, context=arg.DEFAULT) -> ProgressInterface:
        pass

    @abstractmethod
    def get_path_prefix(self) -> str:
        pass

    @abstractmethod
    def get_path_delimiter(self) -> str:
        pass

    @abstractmethod
    def get_path(self) -> str:
        pass

    @abstractmethod
    def get_path_as_list(self) -> list:
        pass

    @abstractmethod
    def get_config_dict(self) -> dict:
        pass

    @abstractmethod
    def get_parent(self) -> OptionalParent:
        pass

    @abstractmethod
    def set_parent(self, parent: SourcedInterface, reset: bool = False, inplace: bool = True):
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def get_children(self) -> dict:
        pass

    @abstractmethod
    def get_child(self, name: str) -> SourcedInterface:
        pass

    @abstractmethod
    def add_child(self, child: SourcedInterface):
        pass

    @abstractmethod
    def forget_child(self, child_or_name: Union[SourcedInterface, str], skip_errors=False):
        pass

    @abstractmethod
    def is_leaf(self) -> bool:
        pass

    @abstractmethod
    def is_root(self) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def is_context() -> bool:
        pass

    @abstractmethod
    def get_context(self):
        pass

    @abstractmethod
    def set_context(self, context, reset=False):
        pass
