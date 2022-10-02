from abc import ABC
from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnectorInterface, LeafConnectorInterface, StructInterface, ContentFormatInterface,
        ItemType, StreamType, ContentType, Context, Stream, Name, Count, Array,
        AUTO, Auto, AutoBool, AutoName, AutoCount, AutoDisplay, AutoConnector, AutoContext,
    )
    from base.functions.arguments import get_name, get_str_from_args_kwargs
    from base.constants.chars import EMPTY, CROP_SUFFIX, ITEMS_DELIMITER, DEFAULT_LINE_LEN
    from content.format.format_classes import ParsedFormat
    from connectors.abstract.abstract_connector import AbstractConnector
    from connectors.mixin.actualize_mixin import ActualizeMixin, DEFAULT_EXAMPLE_COUNT
    from connectors.mixin.connector_format_mixin import ConnectorFormatMixin
    from connectors.mixin.streamable_mixin import StreamableMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ConnectorInterface, LeafConnectorInterface, StructInterface, ContentFormatInterface,
        ItemType, StreamType, ContentType, Context, Stream, Name, Count, Array,
        AUTO, Auto, AutoBool, AutoName, AutoCount, AutoDisplay, AutoConnector, AutoContext,
    )
    from ...base.functions.arguments import get_name, get_str_from_args_kwargs
    from ...base.constants.chars import EMPTY, CROP_SUFFIX, ITEMS_DELIMITER, DEFAULT_LINE_LEN
    from ...content.format.format_classes import ParsedFormat
    from .abstract_connector import AbstractConnector
    from ..mixin.actualize_mixin import ActualizeMixin, DEFAULT_EXAMPLE_COUNT
    from ..mixin.connector_format_mixin import ConnectorFormatMixin
    from ..mixin.streamable_mixin import StreamableMixin

Native = LeafConnectorInterface
Parent = Union[Context, ConnectorInterface]
Links = Optional[dict]

META_MEMBER_MAPPING = dict(_data='streams', _source='parent', _declared_format='content_format')
TEMPORARY_PARTITION_FORMAT = ContentType.JsonFile


class LeafConnector(
    AbstractConnector,
    ActualizeMixin, ConnectorFormatMixin, StreamableMixin,
    LeafConnectorInterface, ABC,
):
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
        content_format = Auto.delayed_acquire(content_format, self._get_detected_format_by_name, name, **kwargs)
        suit_classes = ContentType, ContentFormatInterface, str
        is_deprecated_class = hasattr(content_format, 'get_value') and not isinstance(content_format, suit_classes)
        if is_deprecated_class:
            msg = 'LeafConnector({}, {}): content_format as {} is deprecated, use ContentType or ContentFormat instead'
            self.log(msg.format(name, content_format, content_format.__class__.__name__), level=30)
            content_format = content_format.get_value()
        if isinstance(content_format, str):
            content_format = ContentType(content_format)
        if isinstance(content_format, ContentType):
            content_class = content_format.get_class()
            content_format = content_class(**kwargs)
        elif isinstance(content_format, ContentFormatInterface):
            content_format.set_inplace(**kwargs)
        else:
            if kwargs:
                msg = 'LeafConnector: kwargs allowed for ContentType only, not for {}, got kwargs={}'
                raise ValueError(msg.format(content_format, kwargs))
        assert isinstance(content_format, ContentFormatInterface), 'Expect ContentFormat, got {}'.format(content_format)
        self.set_content_format(content_format, inplace=True)
        self.set_first_line_title(first_line_is_title, verbose=self.is_verbose())
        if struct is not None:
            if struct == AUTO:
                if self.is_accessible():
                    struct = self._get_detected_struct(use_declared_types=False, skip_missing=True)
            if Auto.is_defined(struct, check_name=False):
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
            skip_missing: bool = False,
            verbose: AutoBool = False,
    ) -> Optional[StructInterface]:
        if skip_missing and not self.is_accessible():
            return None
        content_format = self.get_content_format()
        if isinstance(content_format, ContentFormatInterface) and hasattr(content_format, 'is_first_line_title'):
            if content_format.is_first_line_title():
                struct = self.get_struct_from_source(
                    set_struct=set_struct,
                    use_declared_types=use_declared_types,
                    skip_missing=skip_missing,
                    verbose=verbose,
                )
                return struct

    def get_content_format(self, verbose: AutoBool = AUTO) -> ContentFormatInterface:
        detected_format = self.get_detected_format(detect=False, verbose=verbose)
        if Auto.is_defined(detected_format):
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
            verbose: AutoBool = AUTO,
    ) -> ContentFormatInterface:
        if force or (detect and not Auto.is_defined(self._detected_format)):
            self.reset_detected_format(use_declared_types=True, skip_missing=skip_missing, verbose=verbose)
        return self._detected_format

    def set_detected_format(self, content_format: ContentFormatInterface, inplace: bool) -> Native:
        if inplace:
            self._detected_format = content_format
            if not self.get_declared_format():
                return self.set_declared_format(content_format, inplace=True) or self
        else:
            connector = self.make_new(content_format=content_format)
            return self._assume_native(connector)

    def reset_detected_format(
            self,
            use_declared_types: bool = True,
            skip_missing: bool = False,
            verbose: AutoBool = AUTO,
    ) -> Native:
        if self.is_existing(verbose=verbose):
            content_format = self.get_declared_format().copy()
            detected_struct = self.get_struct_from_source(
                set_struct=False,
                use_declared_types=use_declared_types,
                verbose=verbose,
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

    def set_first_line_title(self, first_line_is_title: AutoBool, verbose: AutoBool = AUTO) -> Native:
        declared_format = self.get_declared_format()
        detected_format = self.get_detected_format(detect=False, verbose=verbose)
        if hasattr(declared_format, 'set_first_line_title'):
            declared_format.set_first_line_title(first_line_is_title)
        if hasattr(detected_format, 'set_first_line_title'):
            detected_format.set_first_line_title(first_line_is_title)
        return self

    def get_prev_modification_timestamp(self) -> Optional[float]:
        return self._modification_ts

    def set_prev_modification_timestamp(self, timestamp: float) -> Native:
        self._modification_ts = timestamp
        return self

    def get_expected_count(self) -> Union[int, Auto, None]:
        return self._count

    def set_count(self, count: int) -> Native:
        self._count = count
        return self

    def get_links(self) -> dict:
        return self.get_children()

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

    def has_lines(self, skip_missing: bool = True, verbose: AutoBool = AUTO) -> bool:
        if not self.is_accessible(verbose=verbose):
            if skip_missing:
                return False
            else:
                raise ValueError(f'For receive first line file/object must be existing: {self}')  # ConnectionError
        if not self.is_existing():
            if skip_missing:
                return False
            else:
                raise FileNotFoundError(f'For receive first line file/object must be existing: {self}')
        if self.is_empty():
            if skip_missing:
                return False
            else:
                raise ValueError(f'For receive first line file/object must be non-empty: {self}')
        return True

    def get_first_line(self, close: bool = True, skip_missing: bool = False, verbose: AutoBool = AUTO) -> Optional[str]:
        if not self.has_lines(skip_missing=skip_missing, verbose=verbose):
            return None
        iter_lines = self.get_lines(count=1, skip_first=False, skip_missing=skip_missing, verbose=verbose)
        try:
            first_line = next(iter_lines)
        except StopIteration:
            if skip_missing:
                first_line = None
            else:
                raise ValueError(f'Received empty content: {self}')
        if close:
            self.close()
        return first_line

    def write_stream(self, stream: Stream, verbose: bool = True):
        return self.from_stream(stream, verbose=verbose)

    def copy(self) -> Native:
        copy = self.make_new()
        copy.set_declared_format(self.get_declared_format().copy(), inplace=True)
        copy.set_detected_format(self.get_detected_format().copy(), inplace=True)
        return copy

    def get_items(
            self,
            verbose: AutoBool = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        return self.get_items_of_type(item_type=AUTO, verbose=verbose, step=step)

    def get_items_of_type(
            self,
            item_type: Union[ItemType, Auto],
            verbose: AutoBool = AUTO,
            message: AutoName = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        item_type = Auto.acquire(item_type, self.get_default_item_type())
        verbose = Auto.acquire(verbose, self.is_verbose())
        content_format = self.get_content_format()
        assert isinstance(content_format, ParsedFormat) or hasattr(content_format, 'get_items_from_lines')
        count = self.get_count(allow_slow_mode=False)
        if isinstance(verbose, str):
            if Auto.is_defined(message):
                self.log(verbose, verbose=bool(verbose))
            else:
                message = verbose
        elif (count or 0) > 0:
            template = '{count} lines expected from file {name}...'
            msg = template.format(count=count, name=self.get_name())
            self.log(msg, verbose=verbose)
        lines = self.get_lines(skip_first=self.is_first_line_title(), step=step, verbose=verbose, message=message)
        items = content_format.get_items_from_lines(lines, item_type=item_type)
        return items

    def map(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        if inplace and isinstance(self.get_items(), list):
            return self._apply_map_inplace(function) or self
        else:
            items = self._get_mapped_items(function, flat=False)
            return self.set_items(items, count=self.get_count(), inplace=inplace)

    def filter(self, *args, item_type: ItemType = ItemType.Auto, skip_errors: bool = False, **kwargs) -> Stream:
        item_type = Auto.delayed_acquire(item_type, self.get_item_type)
        stream_type = self.get_stream_type()
        assert isinstance(stream_type, StreamType), f'Expected StreamType, got {stream_type}'
        filtered_items = self._get_filtered_items(*args, item_type=item_type, skip_errors=skip_errors, **kwargs)
        stream = self.to_stream(data=filtered_items, stream_type=stream_type)
        return self._assume_stream(stream)

    def skip(self, count: int = 1, inplace: bool = False) -> Optional[Native]:
        if self.get_count() and count >= self.get_count():
            items = list()
        else:
            items = self.get_items()[count:] if self.is_in_memory() else self._get_second_items(count)
        result_count = None
        if self._has_count_attribute():
            old_count = self.get_count()
            if old_count:
                result_count = old_count - count
                if result_count < 0:
                    result_count = 0
        return self.set_items(items, count=result_count, inplace=inplace)

    def get_one_item(self):
        if self.is_sequence() and self.has_items():
            return self.get_list()[0]
        for i in self.get_iter():
            return i

    def get_str_headers(self, actualize: bool = False) -> Generator:
        cls = self.__class__.__name__
        name = repr(self.get_name())
        shape = self.get_shape_repr(actualize=actualize)
        str_header = f'{cls}({name}) {shape}'
        if len(str_header) > DEFAULT_LINE_LEN:
            str_header = str_header[:DEFAULT_LINE_LEN - len(CROP_SUFFIX)] + CROP_SUFFIX
        yield str_header

    def describe(
            self,
            *filter_args,
            count: Optional[int] = DEFAULT_EXAMPLE_COUNT,
            columns: Optional[Array] = None,
            show_header: bool = True,
            safe_filter: bool = True,
            actualize: AutoBool = AUTO,
            comment: Optional[str] = None,
            display: AutoDisplay = AUTO,
            **filter_kwargs
    ) -> Native:
        display = self.get_display(display)
        if show_header:
            display.display_paragraph(self.get_name(), level=1)
            display.display_paragraph(self.get_str_headers(actualize=False))
        if comment:
            display.display_paragraph(comment)
        struct = self.get_struct()
        if show_header or struct:
            struct_title, example_item, example_stream, example_comment = self._prepare_examples_with_title(
                *filter_args, **filter_kwargs, safe_filter=safe_filter,
                example_row_count=count, actualize=actualize,
                verbose=False,
            )
            display.append(struct_title)
            if self.get_invalid_fields_count():
                line = 'Invalid columns: {}'.format(get_str_from_args_kwargs(*self.get_invalid_columns()))
                display.append(line)
            display.display_paragraph()
        else:
            example_item, example_stream, example_comment = None, None, None
        if isinstance(struct, StructInterface) or hasattr(struct, 'describe'):
            struct.describe(
                show_header=False,
                example=example_item,
                comment=example_comment,
                display=display,
            )
        elif struct:
            display.append(f'[TYPE_ERROR] Expected struct as StructInterface, got {struct} instead')
        else:
            display.append('Struct is not defined.')
        if example_stream and count:
            display.display_paragraph('Example', level=3)
            if example_comment:
                display.display_paragraph(example_comment)
            example_records, example_columns = self._get_demo_records_and_columns(
                count=count,
                example=example_stream,
                columns=columns,
            )
            display.display_sheet(records=example_records, columns=example_columns)
        display.display_paragraph()
        return self

    @staticmethod
    def _assume_native(connector) -> Native:
        return connector
