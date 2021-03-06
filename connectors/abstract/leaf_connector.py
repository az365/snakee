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

    @staticmethod
    def is_root():
        return False

    @staticmethod
    def is_leaf():
        return True

    @staticmethod
    def is_folder():
        return False

    def has_hierarchy(self):
        return False

    @abstractmethod
    def is_existing(self):
        pass

    def check(self, must_exists=True):
        if must_exists:
            assert self.is_existing(), 'object {} must exists'.format(self.get_name())

    def write_stream(self, stream):
        return self.from_stream()

    @abstractmethod
    def from_stream(self, stream):
        pass

    @abstractmethod
    def to_stream(self, **kwargs):
        pass
