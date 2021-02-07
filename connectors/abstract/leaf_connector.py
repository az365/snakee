from abc import abstractmethod

try:  # Assume we're a sub-module in a package.
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import connector_classes as ct


class LeafConnector(ct.AbstractConnector):
    def __init__(
            self,
            name,
            parent=None,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )

    def is_root(self):
        return False

    def has_hierarchy(self):
        return False

    @abstractmethod
    def is_existing(self):
        pass

    @abstractmethod
    def from_stream(self, stream):
        pass

    @abstractmethod
    def to_stream(self, stream_type):
        pass
