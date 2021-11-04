from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from connectors.interfaces.connector_interface import ConnectorInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams.interfaces.abstract_stream_interface import StreamInterface
    from .connector_interface import ConnectorInterface


class LeafConnectorInterface(ConnectorInterface, ABC):
    @abstractmethod
    def is_existing(self) -> bool:
        pass

    @abstractmethod
    def check(self, must_exists: bool = True):
        pass

    @abstractmethod
    def write_stream(self, stream: StreamInterface, verbose: bool = True):
        pass

    @abstractmethod
    def from_stream(self, stream: StreamInterface):
        pass

    @abstractmethod
    def to_stream(self, **kwargs) -> StreamInterface:
        pass