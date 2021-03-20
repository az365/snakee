from abc import ABC, abstractmethod
from typing import Optional, Union, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.base_interface import BaseInterface
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .base_interface import BaseInterface
    from ...streams.interfaces.abstract_stream_interface import StreamInterface
    from ...loggers.logger_interface import LoggerInterface
    from ...loggers.extended_logger_interface import ExtendedLoggerInterface
    from ...loggers.selection_logger_interface import SelectionLoggerInterface

Logger = Union[LoggerInterface, ExtendedLoggerInterface]
Connector = Optional[BaseInterface]
Stream = Optional[StreamInterface]
Child = Union[Logger, Connector, Stream]


class ContextInterface(BaseInterface, ABC):
    def get_context(self) -> BaseInterface:
        return self

    def set_context(self, context, reset=False):
        assert not reset, 'Context cannot be replaced'

    @staticmethod
    def is_context() -> bool:
        return True

    def get_parent(self):
        return None

    def is_root(self) -> bool:
        return True

    def is_leaf(self) -> bool:
        return False

    @abstractmethod
    def get_logger(self, create_if_not_yet=True) -> LoggerInterface:
        pass

    @abstractmethod
    def set_logger(self, logger):
        pass

    @abstractmethod
    def get_selection_logger(self, *args, **kwargs) -> SelectionLoggerInterface:
        pass

    @abstractmethod
    def get_new_selection_logger(self, name, **kwargs) -> SelectionLoggerInterface:
        pass

    @abstractmethod
    def log(self, msg: str, level: int, end: str, verbose: bool):
        pass

    def set_parent(self, parent, reset=False, inplace=False):
        pass

    @abstractmethod
    def get_stream(self, name: str) -> Stream:
        pass

    @abstractmethod
    def get_connection(self, name: str) -> Connector:
        pass

    @abstractmethod
    def conn(self, conn, name: str, check: bool, redefine: bool, **kwargs) -> Connector:
        pass

    @abstractmethod
    def stream(self, stream_type, name: str, check: bool, **kwargs) -> Stream:
        pass

    @abstractmethod
    def rename_stream(self, old_name: str, new_name: str):
        pass

    @abstractmethod
    def get_local_storage(self, name: str) -> Connector:
        pass

    @abstractmethod
    def get_job_folder(self) -> Connector:
        pass

    @abstractmethod
    def get_tmp_folder(self) -> Connector:
        pass

    @abstractmethod
    def close_conn(self, name: str, recursively: bool, verbose: bool) -> int:
        pass

    @abstractmethod
    def close_stream(self, name, recursively=False, verbose=True) -> tuple:
        pass

    @abstractmethod
    def forget_conn(self, name, recursively=True, verbose=True) -> int:
        pass

    @abstractmethod
    def forget_stream(self, name, recursively=True, verbose=True) -> int:
        pass

    @abstractmethod
    def close_all_conns(self, recursively=False, verbose=True) -> int:
        pass

    @abstractmethod
    def close_all_streams(self, recursively=False, verbose=True) -> tuple:
        pass

    @abstractmethod
    def close(self, verbose=True) -> int:
        pass

    @abstractmethod
    def forget_all_conns(self, recursively=False) -> NoReturn:
        pass

    @abstractmethod
    def forget_all_streams(self, recursively=False) -> NoReturn:
        pass

    @abstractmethod
    def forget_all_children(self) -> NoReturn:
        pass
