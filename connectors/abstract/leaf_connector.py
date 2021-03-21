from abc import ABC, abstractmethod
from typing import Optional, Union

try:  # Assume we're a sub-module in a package.
    from connectors.abstract.abstract_connector import AbstractConnector
    from base.interfaces.context_interface import ContextInterface
    from streams.interfaces.abstract_stream_interface import StreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..abstract.abstract_connector import AbstractConnector
    from base.interfaces.context_interface import ContextInterface
    from ...streams.interfaces.abstract_stream_interface import StreamInterface

Context = Optional[ContextInterface]
Parent = Union[Context, AbstractConnector]
Stream = StreamInterface
Links = Optional[dict]

META_MEMBER_MAPPING = dict(_data='streams', _source='parent')


class LeafConnector(AbstractConnector, ABC):
    def __init__(self, name, parent: Parent = None, streams: Links = None):
        super().__init__(name=name, parent=parent, children=streams)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    def is_root(self) -> bool:
        return False

    @staticmethod
    def is_storage() -> bool:
        return False

    def is_leaf(self) -> bool:
        return True

    @staticmethod
    def is_folder():
        return False

    @staticmethod
    def has_hierarchy():
        return False

    @abstractmethod
    def is_existing(self):
        pass

    def check(self, must_exists=True):
        if must_exists:
            assert self.is_existing(), 'object {} must exists'.format(self.get_name())

    def write_stream(self, stream: StreamInterface):
        return self.from_stream(stream)

    @abstractmethod
    def from_stream(self, stream):
        pass

    @abstractmethod
    def to_stream(self, **kwargs) -> StreamInterface:
        pass
