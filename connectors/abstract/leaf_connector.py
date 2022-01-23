from abc import ABC
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
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
TEMPORARY_PARTITION_FORMAT = ContentType.JsonFile


class LeafConnector(AbstractConnector, ConnectorFormatMixin, StreamableMixin, LeafConnectorInterface, ABC):
    def __init__(
            self,
            name: Name,
            content_format: Union[ContentFormatInterface, ContentType, Auto] = AUTO,
            struct: Union[StructInterface, Auto, None] = AUTO,
            first_line_is_title: AutoBool = AUTO,
            parent: Parent = None,
            context: AutoContext = AUTO,
            streams: Links = None,
            expected_count: AutoCount = AUTO,
            caption: Optional[str] = None,
            verbose: AutoBool = AUTO,
            **kwargs
    ):
        self._declared_format = None
        self._detected_format = None
        self._modification_ts = None
        self._count = expected_count
        self._caption = caption
        super().__init__(name=name, parent=parent, context=context, children=streams, verbose=verbose)
        content_format = arg.delayed_acquire(content_format, self._get_detected_format_by_name, name, **kwargs)
        suit_classes = ContentType, ContentFormatInterface
        is_deprecated_class = hasattr(content_format, 'get_value') and not isinstance(content_format, suit_classes)
        if is_deprecated_class:
            msg = 'LeafConnector({}, {}): content_format as {} is deprecated, use ContentType or ContentFormat instead'
            self.log(msg.format(name, content_format, content_format.__class__.__name__), level=30)
            content_format = content_format.get_value()
        if isinstance(content_format, str):
            content_format = ContentType(content_format)  # ContentType.detect(content_format) ?
        if isinstance(content_format, ContentType):  # tmp fix
            content_class = content_format.get_class()
            content_format = content_class(**kwargs)
        elif isinstance(content_format, ContentFormatInterface):
            content_format.set_inplace(**kwargs)
        else:
            if kwargs:
                msg = 'LeafConnector: kwargs allowed for ContentType only, nor for{}, got kwargs={}'
                raise ValueError(msg.format(content_format, kwargs))
        assert isinstance(content_format, ContentFormatInterface), 'Expect ContentFormat, got {}'.format(content_format)
        self.set_content_format(content_format, inplace=True)
        self.set_first_line_title(first_line_is_title)
        if struct is not None:
            if struct == AUTO:
                struct = self._get_detected_struct(use_declared_types=False)
            if arg.is_defined(struct, check_name=False):
                self.set_struct(struct, inplace=True)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    @staticmethod
    def _get_detected_format_by_name(name: Union[str, int], **kwargs) -> ContentFormatInterface:
        is_temporary_partition = isinstance(name, int)
        if is_temporary_partition:
            content_class = TEMPORARY_PARTITION_FORMAT.get_class()
        else:
            content_type = ContentType.detect_by_name(name)
            assert content_type, 'Can not detect content type by name: {}'.format(name)
            content_class = content_type.get_class()
        try:
            return content_class(**kwargs)
        except TypeError as e:
            raise TypeError('{}: {}'.format(content_class.__name__, e))

    def _get_detected_struct(
            self,
            set_struct: bool = False,
            use_declared_types: bool = False,
            verbose: AutoBool = False,
    ) -> Optional[StructInterface]:
        content_format = self.get_content_format()
        if isinstance(content_format, ContentFormatInterface) and hasattr(content_format, 'is_first_line_title'):
            if content_format.is_first_line_title():
                struct = self.get_detected_struct_by_title_row(
                    set_struct=set_struct,
                    use_declared_types=use_declared_types,
                    verbose=verbose,
                )
                return struct

    def get_content_format(self) -> ContentFormatInterface:
        detected_format = self.get_detected_format(detect=False)
        if arg.is_defined(detected_format):
            return detected_format
        else:
            return self.get_declared_format()

    def set_content_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        return self.set_declared_format(content_format, inplace=inplace)

    def get_detected_format(
            self,
            detect: bool = True,
            force: bool = False,
            skip_missing: bool = True,
    ) -> ContentFormatInterface:
        if force or (detect and not arg.is_defined(self._detected_format)):
            self.reset_detected_format(use_declared_types=True, skip_missing=skip_missing)
        return self._detected_format

    def set_detected_format(self, content_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        if inplace:
            self._detected_format = content_format
            if not self.get_declared_format():
                self.set_declared_format(content_format, inplace=True)
        else:
            return self.make_new(content_format=content_format)

    def reset_detected_format(self, use_declared_types: bool = True, skip_missing: bool = False) -> Native:
        if self.is_existing():
            content_format = self.get_declared_format().copy()
            detected_struct = self.get_detected_struct_by_title_row(
                set_struct=False,
                use_declared_types=use_declared_types,
            )
            detected_format = content_format.set_struct(detected_struct, inplace=False)
            self.set_detected_format(detected_format, inplace=True)
        elif not skip_missing:
            raise ValueError('LeafConnector.reset_detected_format(): Data object not found: {}'.format(self))
        return self

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

    def set_first_line_title(self, first_line_is_title: AutoBool) -> Native:
        declared_format = self.get_declared_format()
        detected_format = self.get_detected_format(detect=False)
        if hasattr(declared_format, 'set_first_line_title'):
            declared_format.set_first_line_title(first_line_is_title)
        if hasattr(detected_format, 'set_first_line_title'):
            detected_format.set_first_line_title(first_line_is_title)
        return self

    def get_links(self) -> dict:
        return self.get_children()

    def get_caption(self) -> Optional[str]:
        return self._caption

    def set_caption(self, caption: str) -> Native:
        self._caption = caption
        return self

    def is_in_memory(self) -> bool:
        return False

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

    def check(self, must_exists: bool = True) -> Native:
        if must_exists:
            assert self.is_existing(), 'object {} must exists'.format(self.get_name())
        return self

    def write_stream(self, stream: Stream, verbose: bool = True):
        return self.from_stream(stream, verbose=verbose)

    def copy(self) -> Native:
        copy = self.make_new()
        copy.set_declared_format(self.get_declared_format().copy(), inplace=True)
        copy.set_detected_format(self.get_detected_format().copy(), inplace=True)
        return copy
