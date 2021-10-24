from abc import ABC, abstractmethod
from typing import Optional, Iterable, Iterator, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.interfaces.leaf_connector_interface import LeafConnectorInterface
    from items.struct_interface import StructInterface
    from items.item_type import ItemType
    from streams.stream_type import StreamType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..interfaces.leaf_connector_interface import LeafConnectorInterface
    from ...items.struct_interface import StructInterface
    from ...items.item_type import ItemType
    from ...streams.stream_type import StreamType

Native = LeafConnectorInterface
Struct = Union[StructInterface, arg.Auto, None]


class StructFileInterface(LeafConnectorInterface, ABC):
    @abstractmethod
    def get_count(self) -> Optional[int]:
        pass

    @abstractmethod
    def _get_csv_reader(self, lines: Iterable) -> Iterator:
        pass

    @abstractmethod
    def get_initial_struct(self) -> Struct:
        pass

    @abstractmethod
    def set_initial_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def initial_struct(self, struct: Struct) -> Native:
        pass

    @abstractmethod
    def get_struct(self) -> Struct:
        pass

    @abstractmethod
    def set_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def struct(self, struct: Struct) -> Native:
        pass

    @abstractmethod
    def get_content_type(self):
        pass

    @staticmethod
    @abstractmethod
    def get_default_file_extension() -> str:
        pass

    @staticmethod
    @abstractmethod
    def get_default_item_type() -> ItemType:
        pass

    @classmethod
    @abstractmethod
    def get_stream_type(cls) -> StreamType:
        pass

    @abstractmethod
    def get_delimiter(self) -> str:
        pass

    @abstractmethod
    def set_delimiter(self, delimiter: str, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def is_first_line_title(self) -> bool:
        pass

    @abstractmethod
    def is_verbose(self) -> bool:
        pass

    @abstractmethod
    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False, allow_reopen: bool = True,
            check: bool = True, verbose: Union[bool, arg.Auto] = arg.AUTO,
            message: Union[str, arg.Auto] = arg.AUTO, step: Union[int, arg.Auto] = arg.AUTO,
    ) -> Iterable:
        pass

    @abstractmethod
    def close(self) -> int:
        pass

    @abstractmethod
    def is_existing(self) -> bool:
        pass

    @abstractmethod
    def get_stream_kwargs(
            self, data: Union[Iterable, arg.Auto] = arg.AUTO, name: Union[str, arg.Auto] = arg.AUTO,
            verbose: Union[bool, arg.Auto] = arg.AUTO, step: Union[int, arg.Auto] = arg.AUTO,
            **kwargs
    ) -> dict:
        pass
