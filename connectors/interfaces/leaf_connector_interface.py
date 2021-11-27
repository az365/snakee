from abc import ABC, abstractmethod
from typing import Optional, Union

try:  # Assume we're a sub-module in a package.
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from connectors.content_format.content_type import ContentType
    from connectors.interfaces.connector_interface import ConnectorInterface
    from connectors.interfaces.format_interface import ContentFormatInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams.interfaces.abstract_stream_interface import StreamInterface
    from ..content_format.content_type import ContentType
    from .connector_interface import ConnectorInterface
    from .format_interface import ContentFormatInterface

Native = Union[ConnectorInterface, StreamInterface]


class LeafConnectorInterface(ConnectorInterface, StreamInterface, ABC):
    @abstractmethod
    def get_content_type(self) -> ContentType:
        pass

    @abstractmethod
    def get_content_format(self) -> ContentFormatInterface:
        pass

    @abstractmethod
    def set_content_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def get_declared_format(self) -> ContentFormatInterface:
        pass

    @abstractmethod
    def set_declared_format(self, initial_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def get_detected_format(self, detect: bool = True, force: bool = False) -> ContentFormatInterface:
        pass

    @abstractmethod
    def set_detected_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def reset_detected_format(self) -> Native:
        pass

    @abstractmethod
    def is_existing(self) -> bool:
        pass

    @abstractmethod
    def get_first_line(self, close: bool = True) -> Optional[str]:
        pass

    @abstractmethod
    def check(self, must_exists: bool = True) -> Native:
        pass

    @abstractmethod
    def write_stream(self, stream: StreamInterface, verbose: bool = True) -> Native:
        pass

    @abstractmethod
    def from_stream(self, stream: StreamInterface) -> Native:
        pass

    @abstractmethod
    def to_stream(self, **kwargs) -> StreamInterface:
        pass
