from abc import ABC, abstractmethod
from typing import Optional

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.base_interface import BaseInterface
    from base.interfaces.tree_interface import TreeInterface
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .base_interface import BaseInterface
    from .tree_interface import TreeInterface
    from ...loggers.logger_interface import LoggerInterface

Named = Optional[BaseInterface]


class ContextInterface(TreeInterface, ABC):
    def get_context(self):
        return self

    @abstractmethod
    def get_logger(self, create_if_not_yet=True) -> LoggerInterface:
        pass

    @abstractmethod
    def set_logger(self, logger):
        pass

    @abstractmethod
    def get_selection_logger(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_new_selection_logger(self, name, **kwargs):
        pass

    @abstractmethod
    def log(self, msg: str, level: int, end: str, verbose: bool):
        pass

    @abstractmethod
    def get_stream(self, name: str) -> Named:
        pass

    @abstractmethod
    def get_connection(self, name: str) -> Named:
        pass

    @abstractmethod
    def conn(self, conn, name: str, check: bool, redefine: bool, **kwargs) -> Named:
        pass

    @abstractmethod
    def stream(self, stream_type, name: str, check: bool, **kwargs) -> Named:
        pass

    @abstractmethod
    def rename_stream(self, old_name: str, new_name: str):
        pass

    @abstractmethod
    def get_local_storage(self, name: str) -> Named:
        pass

    @abstractmethod
    def get_job_folder(self) -> Named:
        pass

    @abstractmethod
    def get_tmp_folder(self) -> Named:
        pass

    @abstractmethod
    def close_conn(self, name: str, recursively: bool, verbose: bool) -> int:
        pass

    @abstractmethod
    def close_stream(self, name, recursively=False, verbose=True) -> int:
        pass

    @abstractmethod
    def forget_conn(self, name, recursively=True, verbose=True):
        pass

    @abstractmethod
    def forget_stream(self, name, recursively=True, verbose=True):
        pass

    @abstractmethod
    def close_all_conns(self, recursively=False, verbose=True) -> int:
        pass

    @abstractmethod
    def close_all_streams(self, recursively=False, verbose=True) -> int:
        pass

    @abstractmethod
    def close(self, verbose=True) -> int:
        pass

    @abstractmethod
    def forget_all_conns(self, recursively=False):
        pass

    @abstractmethod
    def forget_all_streams(self, recursively=False):
        pass

    @abstractmethod
    def forget_all_children(self):
        pass
