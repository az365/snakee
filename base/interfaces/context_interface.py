from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.interfaces.base_interface import BaseInterface
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .base_interface import BaseInterface
    from ...streams.interfaces.abstract_stream_interface import StreamInterface
    from ...loggers.logger_interface import LoggerInterface
    from ...loggers.extended_logger_interface import ExtendedLoggerInterface
    from ...loggers.selection_logger_interface import SelectionLoggerInterface

Native = BaseInterface
LoggingLevel = Optional[int]
Logger = Union[LoggerInterface, ExtendedLoggerInterface]
Connector = Optional[BaseInterface]
Stream = Optional[StreamInterface]
Child = Union[Logger, Connector, Stream]
ChildType = Union[Child, str, Any]
Name = Union[str, int]


class ContextInterface(BaseInterface, ABC):
    def get_context(self) -> Native:
        return self

    @abstractmethod
    def set_context(self, context: Native, reset: bool = False, inplace: bool = True) -> Optional[Native]:
        pass

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
    def get_logger(self, create_if_not_yet: bool = True) -> LoggerInterface:
        pass

    @abstractmethod
    def set_logger(self, logger: Logger, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def get_selection_logger(self, *args, **kwargs) -> SelectionLoggerInterface:
        pass

    @abstractmethod
    def get_new_selection_logger(self, name: Name, **kwargs) -> SelectionLoggerInterface:
        pass

    @abstractmethod
    def log(
            self, msg: str,
            level: LoggingLevel = None,
            end: Optional[str] = None,
            verbose: bool = True,
    ):
        pass

    @abstractmethod
    def get_child(self, name: Name, class_or_type: ChildType = None, deep: bool = True) -> Child:
        pass

    @abstractmethod
    def add_child(self, instance: Child, reset: bool = False, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def set_parent(self, parent: Any, reset: bool = False, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def get_children(self) -> dict:
        pass

    @abstractmethod
    def get_stream(self, name: Name, skip_missing: bool = True) -> Optional[Stream]:
        pass

    @abstractmethod
    def get_connection(self, name: Name, skip_missing: bool = True) -> Optional[Connector]:
        pass

    @abstractmethod
    def conn(
            self,
            conn: Union[Connector, ChildType],
            name: Optional[Name] = None,
            check: bool = True, redefine: bool = True,
            **kwargs
    ) -> Connector:
        pass

    @abstractmethod
    def stream(
            self,
            data: Iterable,
            item_type: Union[Stream, ChildType],
            name: Optional[Name] = None,
            check: bool = True,
            **kwargs
    ) -> Stream:
        pass

    @abstractmethod
    def rename_stream(self, old_name: Name, new_name: Name) -> Stream:
        pass

    @abstractmethod
    def get_local_storage(self, name: Name = 'filesystem', create_if_not_yet: bool = True) -> Connector:
        pass

    @abstractmethod
    def get_job_folder(self) -> Connector:
        pass

    @abstractmethod
    def get_tmp_folder(self) -> Connector:
        pass

    @abstractmethod
    def clear_tmp_files(self, verbose: bool = True) -> int:
        pass

    @abstractmethod
    def close_conn(self, name: Name, recursively: bool = False, verbose: bool = True) -> int:
        pass

    @abstractmethod
    def close_stream(self, name: Name, recursively: bool = False, verbose: bool = True) -> tuple:
        pass

    @abstractmethod
    def forget_conn(self, conn: Union[Name, Connector], recursively=True, skip_errors=False, verbose=True) -> int:
        pass

    @abstractmethod
    def forget_stream(self, stream: Union[Name, Stream], recursively=True, skip_errors=False, verbose=True) -> int:
        pass

    @abstractmethod
    def forget_child(self, name_or_child: Union[Name, Child], recursively=True, skip_errors=False) -> int:
        pass

    @abstractmethod
    def close_all_conns(self, recursively: bool = False, verbose: bool = True) -> int:
        pass

    @abstractmethod
    def close_all_streams(self, recursively: bool = False, verbose: bool = True) -> tuple:
        pass

    @abstractmethod
    def close(self, verbose: bool = True) -> tuple:
        pass

    @abstractmethod
    def forget_all_conns(self, recursively: bool = False, verbose: bool = True) -> int:
        pass

    @abstractmethod
    def forget_all_streams(self, recursively: bool = False, verbose: bool = True) -> int:
        pass

    @abstractmethod
    def forget_all_children(self, verbose: bool = True) -> int:
        pass
