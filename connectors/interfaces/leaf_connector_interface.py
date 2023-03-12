from abc import ABC, abstractmethod
from typing import Optional, Iterable, Iterator, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Count
    from streams.interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from content.items.item_type import ItemType
    from content.format.content_type import ContentType
    from content.format.format_interface import ContentFormatInterface
    from connectors.interfaces.connector_interface import ConnectorInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import Count
    from ...streams.interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from ...content.items.item_type import ItemType
    from ...content.format.content_type import ContentType
    from ...content.format.format_interface import ContentFormatInterface
    from .connector_interface import ConnectorInterface

Native = Union[ConnectorInterface, StreamInterface]


class LeafConnectorInterface(ConnectorInterface, StreamInterface, ABC):
    @abstractmethod
    def get_content_type(self) -> ContentType:
        """Returns type of content (detected from connector format settings)
        as ContentType enum-object with one of possible values:

        TextFile, JsonFile, ColumnFile, CsvFile, TsvFile
        """
        pass

    @abstractmethod
    def get_content_format(self) -> ContentFormatInterface:
        """Returns ContentFormat object
        containing ContentType and other properties, probably including Struct.
        """
        pass

    @abstractmethod
    def set_content_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        """Setting ContentFormat property to connector.
        """
        pass

    @abstractmethod
    def get_declared_format(self) -> ContentFormatInterface:
        """Getting declared ContentFormat.
        Declared format will be applied with next object update.
        Detected format can be different of declared, it's describe actual format of stored data object.
        """
        pass

    @abstractmethod
    def set_declared_format(self, initial_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        """Reset initial expected struct in connector settings."""
        pass

    @abstractmethod
    def get_detected_format(
            self,
            detect: bool = True,
            force: bool = False,
            skip_missing: bool = True,
    ) -> ContentFormatInterface:
        """Getting detected ContentFormat.
        Detected format describe actual format of stored data object.
        Declared can be different of detected, it will be applied with next object update.
        """
        pass

    @abstractmethod
    def set_detected_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        """Reset initial expected struct in connector settings."""
        pass

    @abstractmethod
    def reset_detected_format(self) -> Native:
        """Reset detected_format property by ContentFormat detected by title row of stored data object.
        """
        pass

    @abstractmethod
    def get_struct(self):
        pass

    @abstractmethod
    def get_struct_from_source(
            self,
            set_struct: bool = False,
            use_declared_types: bool = True,
            skip_missing: bool = False,
            verbose: Optional[bool] = None,
    ):
        pass

    @abstractmethod
    def set_first_line_title(self, first_line_is_title: Optional[bool]) -> Native:
        pass

    @abstractmethod
    def get_prev_modification_timestamp(self) -> Optional[float]:
        pass

    @abstractmethod
    def set_prev_modification_timestamp(self, timestamp: float) -> Native:
        pass

    @abstractmethod
    def is_existing(self) -> bool:
        """Checks that file/table is existing in storage/database.
        """
        pass

    @abstractmethod
    def get_expected_count(self) -> Count:
        pass

    @abstractmethod
    def get_count(self, allow_slow_mode: bool = True) -> Optional[int]:
        pass

    def set_count(self, count: int) -> Native:
        pass

    @abstractmethod
    def get_first_line(
            self,
            close: bool = True,
            skip_missing: bool = False,
            verbose: Optional[bool] = None,
    ) -> Optional[str]:
        """Returns raw, unparsed first line of stored data object.
        """
        pass

    @abstractmethod
    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False,
            skip_missing: bool = False,
            allow_reopen: bool = True,
            verbose: Optional[bool] = None,
            message: Optional[str] = None,
            step: Count = None,
    ) -> Iterator[str]:
        pass

    def get_items_of_type(
            self,
            item_type: Optional[ItemType],
            verbose: Optional[bool] = None,
            message: Optional[str] = None,
            step: Count = None,
    ) -> Iterable:
        pass

    @abstractmethod
    def check(self, must_exists: bool = True) -> Native:
        """Validates object and returns same object after validation.
        """
        pass

    @abstractmethod
    def write_stream(self, stream: StreamInterface, verbose: bool = True) -> Native:
        """Write data from provided stream to current data object.
        Alias of from_stream() method.
        """
        pass

    @abstractmethod
    def from_stream(self, stream: StreamInterface) -> Native:
        """Write data from provided stream to current data object.
        Alias of write_stream() method.
        """
        pass

    @abstractmethod
    def to_stream(self, **kwargs) -> StreamInterface:
        """Read data from data object to stream.

        :returns: Stream with data from connected object.
        """
        pass

    @abstractmethod
    def describe(
            self,
            *filter_args,
            count: Optional[int] = DEFAULT_EXAMPLE_COUNT,
            columns: Optional[Iterable] = None,
            show_header: bool = True,
            struct_as_dataframe: bool = False,  # deprecated
            safe_filter: bool = True,
            actualize: Optional[bool] = None,
            **filter_kwargs
    ):
        pass
