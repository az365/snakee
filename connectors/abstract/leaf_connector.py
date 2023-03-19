from abc import ABC
from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnectorInterface, LeafConnectorInterface, StructInterface, ContentFormatInterface, RegularStreamInterface,
        ItemType, StreamType, ContentType, Context, Stream, Name, Count, Columns, Array,
    )
    from base.constants.chars import EMPTY, CROP_SUFFIX, ITEMS_DELIMITER, DEFAULT_LINE_LEN
    from base.functions.arguments import get_name, get_str_from_args_kwargs, get_cropped_text
    from base.functions.errors import get_loc_message, get_type_err_msg
    from content.format.format_classes import ParsedFormat
    from connectors.abstract.abstract_connector import AbstractConnector
    from connectors.mixin.actualize_mixin import ActualizeMixin, DEFAULT_EXAMPLE_COUNT
    from connectors.mixin.connector_format_mixin import ConnectorFormatMixin
    from connectors.mixin.streamable_mixin import StreamableMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ConnectorInterface, LeafConnectorInterface, StructInterface, ContentFormatInterface, RegularStreamInterface,
        ItemType, StreamType, ContentType, Context, Stream, Name, Count, Columns, Array,
    )
    from ...base.constants.chars import EMPTY, CROP_SUFFIX, ITEMS_DELIMITER, DEFAULT_LINE_LEN
    from ...base.functions.arguments import get_name, get_str_from_args_kwargs, get_cropped_text
    from ...base.functions.errors import get_loc_message, get_type_err_msg
    from ...content.format.format_classes import ParsedFormat
    from .abstract_connector import AbstractConnector
    from ..mixin.actualize_mixin import ActualizeMixin, DEFAULT_EXAMPLE_COUNT
    from ..mixin.connector_format_mixin import ConnectorFormatMixin
    from ..mixin.streamable_mixin import StreamableMixin

Native = LeafConnectorInterface
Parent = Union[Context, ConnectorInterface]
Links = Optional[dict]
ContentFormat = Union[ContentFormatInterface, ContentType]
CONTENT_FORMAT_CLASSES = ContentFormatInterface, ContentType

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
            content_format: Optional[ContentFormat] = None,
            struct: Optional[StructInterface] = None,
            detect_struct: bool = True,
            first_line_is_title: Optional[bool] = None,
            parent: Parent = None,
            context: Context = None,
            streams: Links = None,
            expected_count: Count = None,
            caption: Optional[str] = None,
            verbose: Optional[bool] = None,
            **kwargs
    ):
        self._declared_format = None
        self._detected_format = None
        self._modification_ts = None
        self._count = expected_count
        self._caption = caption
        super().__init__(name=name, parent=parent, context=context, children=streams, verbose=verbose)
        if content_format is None:
            content_format = self._get_detected_format_by_name(name, **kwargs)
        suit_classes = ContentType, ContentFormatInterface, str
        is_deprecated_class = hasattr(content_format, 'get_value') and not isinstance(content_format, suit_classes)
        if is_deprecated_class:
            received = content_format.__class__.__name__
            expected = ' or '.join(map(get_name, CONTENT_FORMAT_CLASSES))
            msg = f'content_format as {received} is deprecated, use {expected} instead'
            self.log(get_loc_message(msg, caller=LeafConnector, args=(name, content_format)), level=30)
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
                msg = f'kwargs allowed for ContentType only, not for {content_format}, got kwargs={kwargs}'
                raise ValueError(get_loc_message(msg, caller=LeafConnector))
        if not isinstance(content_format, ContentFormatInterface):
            msg = get_type_err_msg(expected=ContentFormatInterface, got=content_format, arg='content_format')
            raise TypeError(msg)
        self.set_content_format(content_format, inplace=True)
        self.set_first_line_title(first_line_is_title, verbose=self.is_verbose())
        if detect_struct and struct is None:
            struct = self._get_detected_struct(use_declared_types=False, skip_missing=True)
        if struct is not None:
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
            if content_type:
                content_class = content_type.get_class()
            else:
                msg = f'Can not detect content type by name: {name}'
                raise ValueError(get_loc_message(msg))
        try:
            return content_class(**kwargs)
        except TypeError as e:
            msg = f'{content_class.__name__}: {e}'
            raise TypeError(get_loc_message(msg))

    def _get_detected_struct(
            self,
            set_struct: bool = False,
            use_declared_types: bool = False,
            skip_missing: bool = False,
            verbose: Optional[bool] = False,
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

    def get_content_format(self, verbose: Optional[bool] = None) -> ContentFormatInterface:
        detected_format = self.get_detected_format(detect=False, verbose=verbose)
        if detected_format is not None:
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
            verbose: Optional[bool] = None,
    ) -> ContentFormatInterface:
        if force or (detect and self._detected_format is None):
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
            verbose: Optional[bool] = None,
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
            msg = f'Data object not found: {self}'
            raise ValueError(get_loc_message(msg))
        return self

    def get_declared_format(self) -> ContentFormatInterface:
        return self._declared_format

    def set_declared_format(self, initial_format: ContentFormatInterface, inplace: bool) -> Optional[Native]:
        if inplace:
            self._declared_format = initial_format.copy()
        else:
            new = self.copy()
            assert isinstance(new, LeafConnector), get_type_err_msg(expected=LeafConnector, got=new, arg='self.copy()')
            new.set_declared_format(initial_format, inplace=True)
            return new

    def set_first_line_title(self, first_line_is_title: Optional[bool], verbose: Optional[bool] = None) -> Native:
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

    def get_expected_count(self) -> Count:
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
        if must_exists and not self.is_existing():
            msg = f'For check object {self.get_name()} must exists'
            raise FileNotFoundError(get_loc_message(msg))  # ConnectionError
        return self

    def has_lines(self, skip_missing: bool = True, verbose: Optional[bool] = None) -> bool:
        if not self.is_accessible(verbose=verbose):
            if skip_missing:
                return False
            else:
                msg = f'For receive first line file/object must be existing: {self}'
                raise ValueError(get_loc_message(msg))  # ConnectionError
        if not self.is_existing():
            if skip_missing:
                return False
            else:
                msg = f'For receive first line file/object must be existing: {self}'
                raise FileNotFoundError(get_loc_message(msg))
        if self.is_empty():
            if skip_missing:
                return False
            else:
                msg = f'For receive first line file/object must be non-empty: {self}'
                raise ValueError(get_loc_message(msg))
        return True

    def get_first_line(
            self,
            close: bool = True,
            skip_missing: bool = False,
            verbose: Optional[bool] = None,
    ) -> Optional[str]:
        if not self.has_lines(skip_missing=skip_missing, verbose=verbose):
            return None
        iter_lines = self.get_lines(count=1, skip_first=False, skip_missing=skip_missing, verbose=verbose)
        try:
            first_line = next(iter_lines)
        except StopIteration:
            if skip_missing:
                first_line = None
            else:
                msg = f'Received empty content: {self}'
                raise ValueError(get_loc_message(msg))
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

    def _get_demo_example(
            self,
            count: Count = DEFAULT_EXAMPLE_COUNT,
            filters: Columns = None,
            columns: Columns = None,
            example: Optional[Stream] = None,
    ) -> Optional[Stream]:
        if example is not None:
            stream = example
        elif self.is_existing() and self.is_accessible():
            stream = self
        if filters is not None:
            stream = stream.filter(*filters)
        if count is not None:
            stream = stream.take(count)
        if columns is not None:
            if hasattr(stream, 'select'):
                stream = stream.select(columns)
        stream = stream.collect()
        self.close()
        return stream

    def get_items(self, verbose: Optional[bool] = None, step: Count = None) -> Iterable:
        return self.get_items_of_type(item_type=ItemType.Auto, verbose=verbose, step=step)

    def get_items_of_type(
            self,
            item_type: ItemType,
            verbose: Optional[bool] = None,
            message: Optional[str] = None,
            step: Count = None,
    ) -> Iterable:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_default_item_type()
        if verbose is None:
            verbose = self.is_verbose()
        content_format = self.get_content_format()
        if not (isinstance(content_format, ParsedFormat) or hasattr(content_format, 'get_items_from_lines')):
            msg = get_type_err_msg(expected=ParsedFormat, got=content_format, arg='self.get_content_format()')
            raise ValueError(msg)
        count = self.get_count(allow_slow_mode=False)
        if isinstance(verbose, str):
            if message is not None:
                self.log(verbose, verbose=bool(verbose))
            else:
                message = verbose
        elif (count or 0) > 0:
            file_name = self.get_name()
            self.log(f'{count} lines expected from file {file_name}...', verbose=verbose)
        lines = self.get_lines(skip_first=self.is_first_line_title(), step=step, verbose=verbose, message=message)
        items = content_format.get_items_from_lines(lines, item_type=item_type)
        return items

    def map(self, function: Callable, inplace: bool = False) -> Stream:
        if inplace and isinstance(self.get_items(), list):
            return self._apply_map_inplace(function) or self
        else:
            items = self._get_mapped_items(function, flat=False)
            return self.set_items(items, count=self.get_count(), inplace=inplace)

    def filter(self, *args, item_type: ItemType = ItemType.Auto, skip_errors: bool = False, **kwargs) -> Stream:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        filtered_items = self._get_filtered_items(*args, item_type=item_type, skip_errors=skip_errors, **kwargs)
        stream = self.to_stream(data=filtered_items, stream_type=item_type)
        return self._assume_stream(stream)

    def skip(self, count: int = 1, inplace: bool = False) -> Stream:
        stream = super().skip(count, inplace=inplace)
        struct = self.get_struct()
        if struct is not None:
            if isinstance(stream, RegularStreamInterface) or hasattr(stream, 'set_struct'):
                stream.set_struct(struct, check=False, inplace=True)
        return self._assume_stream(stream)

    def get_one_item(self):
        for i in self.get_items():
            return i

    def get_str_headers(self, actualize: bool = False) -> Generator:
        cls_name = self.__class__.__name__
        obj_name = repr(self.get_name())
        shape = self.get_shape_repr(actualize=actualize)
        str_header = f'{cls_name}({obj_name}) {shape}'
        yield get_cropped_text(str_header)

    @staticmethod
    def _assume_native(connector) -> Native:
        return connector
