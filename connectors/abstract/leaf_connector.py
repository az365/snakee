from abc import ABC
from typing import Optional, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        ConnectorInterface, LeafConnectorInterface, Context, Stream,
        ContentFormatInterface, ContentType,
        AUTO, Auto, AutoBool, AutoCount, AutoConnector, AutoContext, Name, StructInterface,
    )
    from connectors.abstract.abstract_connector import AbstractConnector
    from connectors.mixin.connector_format_mixin import ConnectorFormatMixin
    from connectors.mixin.streamable_mixin import StreamableMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        ConnectorInterface, LeafConnectorInterface, Context, Stream,
        ContentFormatInterface, ContentType,
        AUTO, Auto, AutoBool, AutoCount, AutoConnector, AutoContext, Name, StructInterface,
    )
    from .abstract_connector import AbstractConnector
    from ..mixin.connector_format_mixin import ConnectorFormatMixin
    from ..mixin.streamable_mixin import StreamableMixin

Native = LeafConnectorInterface
Parent = Union[Context, ConnectorInterface]
Links = Optional[dict]

META_MEMBER_MAPPING = dict(_data='streams', _source='parent', _declared_format='content_format')


class LeafConnector(AbstractConnector, ConnectorFormatMixin, StreamableMixin, LeafConnectorInterface, ABC):
    def __init__(
            self,
            name: Name,
            content_format: Union[ContentFormatInterface, Auto] = AUTO,
            struct: Union[StructInterface, Auto, None] = AUTO,
            parent: Parent = None,
            context: AutoContext = AUTO,
            streams: Links = None,
            expected_count: AutoCount = AUTO,
            verbose: AutoBool = AUTO,
    ):
        self._declared_format = None
        self._detected_format = None
        self._modification_ts = None
        self._count = expected_count
        super().__init__(name=name, parent=parent, context=context, children=streams, verbose=verbose)
        content_format = arg.delayed_acquire(content_format, self._get_detected_format_by_name, name)
        assert isinstance(content_format, ContentFormatInterface)
        self.set_content_format(content_format, inplace=True)
        if struct is not None:
            if struct == AUTO:
                struct = self._get_detected_struct()
            if arg.is_defined(struct, check_name=False):
                self.set_struct(struct, inplace=True)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    @staticmethod
    def _get_detected_format_by_name(name: str) -> ContentFormatInterface:
        return ContentType.detect_by_name(name)

    def _get_detected_struct(self, set_struct: bool = False, verbose: AutoBool = AUTO) -> StructInterface:
        content_format = self.get_content_format()
        if isinstance(content_format, ContentFormatInterface) and hasattr(content_format, 'is_first_line_title'):
            if content_format.is_first_line_title():
                struct = self.get_detected_struct_by_title_row(set_struct=set_struct, verbose=verbose)
                return struct

    def get_content_format(self) -> ContentFormatInterface:
        return self.get_detected_format()

    def set_content_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        return self.set_detected_format(content_format=content_format, inplace=inplace)

    def get_detected_format(self) -> ContentFormatInterface:
        return self._detected_format

    def set_detected_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        if inplace:
            self._detected_format = content_format
            if not self.get_declared_format():
                self.set_declared_format(content_format, inplace=True)
        else:
            return self.make_new(content_format=content_format)

    def get_declared_format(self) -> ContentFormatInterface:
        return self._declared_format

    def set_declared_format(self, initial_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        if inplace:
            self._declared_format = initial_format.copy()
        else:
            new = self.copy()
            assert isinstance(new, LeafConnector)
            new.set_declared_format(initial_format, inplace=True)
            return new

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

    def check(self, must_exists=True):
        if must_exists:
            assert self.is_existing(), 'object {} must exists'.format(self.get_name())

    def write_stream(self, stream: Stream, verbose: bool = True):
        return self.from_stream(stream, verbose=verbose)
