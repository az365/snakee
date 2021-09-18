from abc import abstractmethod
from typing import Callable, Iterable

try: # Assume we're a sub-module in a package.
    from connectors.interfaces.connector_interface import ConnectorInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces.connector_interface import ConnectorInterface

Count = int


class TemporaryLocationInterface(ConnectorInterface):
    @abstractmethod
    def get_str_mask_template(self) -> str:
        pass

    @abstractmethod
    def mask(self, mask: str) -> ConnectorInterface:
        pass

    @abstractmethod
    def stream_mask(self, stream_or_name, *args, **kwargs) -> ConnectorInterface:
        pass

    @abstractmethod
    def clear_all(self, forget: bool = True, verbose: bool = True) -> Count:
        pass


class TemporaryFilesMaskInterface(ConnectorInterface):
    @abstractmethod
    def get_encoding(self) -> str:
        pass

    @abstractmethod
    def remove_all(self, forget: bool = True, log: bool = True, verbose: bool = False) -> Count:
        pass

    @abstractmethod
    def get_files(self) -> Iterable:
        pass

    @abstractmethod
    def get_items(self, how: str = 'records', *args, **kwargs) -> Iterable:
        pass

    @abstractmethod
    def get_items_count(self) -> Count:
        pass

    @abstractmethod
    def get_files_count(self) -> Count:
        pass

    @abstractmethod
    def get_count(self, count_items: bool = True) -> Count:
        pass

    @abstractmethod
    def get_sorted_items(
            self,
            key_function: Callable,
            reverse: bool = False,
            return_count: bool = False,
            remove_after: bool = False,
            verbose: bool = True,
    ):
        pass
