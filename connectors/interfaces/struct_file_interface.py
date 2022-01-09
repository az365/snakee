from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.interfaces.leaf_connector_interface import LeafConnectorInterface
    from content.struct.struct_interface import StructInterface
    from content.items.item_type import ItemType
    from streams.stream_type import StreamType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..interfaces.leaf_connector_interface import LeafConnectorInterface
    from ...content.struct.struct_interface import StructInterface
    from ...content.items.item_type import ItemType
    from ...streams.stream_type import StreamType

Native = LeafConnectorInterface
Struct = Union[StructInterface, arg.Auto, None]


class StructFileInterface(LeafConnectorInterface, ABC):
    @abstractmethod
    def get_count(self) -> Optional[int]:
        """Returns expected count of items in file.

        :returns: integer count of items if it is known, otherwise None (if file is compressed or not exists)
        """
        pass

    @abstractmethod
    def get_initial_struct(self) -> Struct:
        """Returns initial expected struct from connector settings."""
        pass

    @abstractmethod
    def set_initial_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        """Reset initial expected struct in connector settings."""
        pass

    @abstractmethod
    def initial_struct(self, struct: Struct) -> Native:
        """Reset initial expected struct in connector settings."""
        pass

    @abstractmethod
    def get_struct(self) -> Struct:
        """Getting actual struct for file/object.
        If file/object is existing this method checks actual struct in actual file data.
        Otherwise, returns initial expected struct from connector settings.
        """
        pass

    @abstractmethod
    def set_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        """Reset actual struct in file connector settings."""
        pass

    @abstractmethod
    def struct(self, struct: Struct) -> Native:
        """Reset actual struct in file connector settings."""
        pass

    @abstractmethod
    def get_content_type(self):
        """Returns ContentType detected from file format settings."""
        pass

    @staticmethod
    @abstractmethod
    def get_default_file_extension() -> str:
        """Returns expected (recommended) file extension for files with this content format."""
        pass

    @staticmethod
    @abstractmethod
    def get_default_item_type() -> ItemType:
        """Returns ItemType expected while parsing this file/content."""
        pass

    @classmethod
    @abstractmethod
    def get_stream_type(cls) -> StreamType:
        """Returns default (recommended) StreamType for this type of file/content."""
        pass

    @abstractmethod
    def get_delimiter(self) -> str:
        """Returns delimiter setting for CSV/TSV file (i.e. space, tabulation, semicolon, comma with/without space)."""
        pass

    @abstractmethod
    def set_delimiter(self, delimiter: str, inplace: bool) -> Optional[Native]:
        """Change delimiter setting for CSV/TSV file (i.e. space, tabulation, semicolon, comma with/without space)."""
        pass

    @abstractmethod
    def is_first_line_title(self) -> bool:
        """Checks is file format settings provides flag that
        first line of CSV/TSV file contains column titles instead of data.
        """
        pass

    @abstractmethod
    def is_verbose(self) -> bool:
        """Checks default verbose settings for this file.
        Can be used by default for methods with verbose argument.

        :returns: boolean flag with verbose setting.
        """
        pass

    @abstractmethod
    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False, allow_reopen: bool = True,
            check: bool = True, verbose: Union[bool, arg.Auto] = arg.AUTO,
            message: Union[str, arg.Auto] = arg.AUTO, step: Union[int, arg.Auto] = arg.AUTO,
    ) -> Iterable:
        """Get raw (not parsed) lines from file.

        :returns: Generator of strings with raw (not parsed) lines from file/
        """
        pass

    @abstractmethod
    def close(self) -> int:
        """Close connection(s) or fileholder(s) if it's opened.

        :returns: count of closed connections.
        """
        pass

    @abstractmethod
    def is_existing(self) -> bool:
        """Checks that file is existing in filesystem."""
        pass

    @abstractmethod
    def get_stream_kwargs(
            self,
            data: Union[Iterable, arg.Auto] = arg.AUTO,
            name: Union[str, arg.Auto] = arg.AUTO,
            verbose: Union[bool, arg.Auto] = arg.AUTO,
            step: Union[int, arg.Auto] = arg.AUTO,
            **kwargs
    ) -> dict:
        """Returns kwargs for stream builder call.

        :returns: dict with kwargs for provide in stream builder arguments, i.e. *Stream(**self.get_stream_kwargs(data))
        """
        pass
