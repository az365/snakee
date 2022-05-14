from abc import ABC
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        LeafConnectorInterface, StructInterface, IterableStreamInterface,
        ItemType, StreamType, ContentType,
        AUTO, Auto, AutoBool,
    )
    from content.struct.struct_mixin import StructMixin
    from content.format.text_format import AbstractFormat, ParsedFormat, TextFormat
    from content.format.lean_format import LeanFormat, ColumnarFormat, FlatStructFormat
    from functions.secondary import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        LeafConnectorInterface, StructInterface, IterableStreamInterface,
        ItemType, StreamType, ContentType,
        AUTO, Auto, AutoBool,
    )
    from ...content.struct.struct_mixin import StructMixin
    from ...content.format.text_format import AbstractFormat, ParsedFormat, TextFormat
    from ...content.format.lean_format import LeanFormat, ColumnarFormat, FlatStructFormat
    from ...functions.secondary import item_functions as fs

Native = Union[LeafConnectorInterface, IterableStreamInterface]
Struct = Optional[StructInterface]


class ConnectorFormatMixin(StructMixin, LeafConnectorInterface, ABC):
    def get_initial_struct(self) -> Struct:
        initial_format = self.get_declared_format()
        if isinstance(initial_format, FlatStructFormat) or hasattr(initial_format, 'get_struct'):
            return initial_format.get_struct()

    def set_initial_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        struct = self._get_native_struct(struct).copy()
        initial_format = self.get_declared_format()
        if inplace:
            if isinstance(initial_format, (LeanFormat, ColumnarFormat)) or hasattr(initial_format, 'set_struct'):
                initial_format.set_struct(struct, inplace=True)
            else:
                raise TypeError('Cannot set struct for {}: method not supported (by design)'.format(initial_format))
        else:
            copy = self.make_new()
            assert isinstance(copy, ConnectorFormatMixin)
            copy.set_initial_struct(struct, inplace=True)
            return self._assume_native(copy)

    def initial_struct(self, struct: Struct) -> Native:
        struct = self._get_native_struct(struct).copy()
        self.set_initial_struct(struct, inplace=True)
        return self._assume_native(self)

    def get_struct(self, verbose: AutoBool = AUTO) -> Struct:
        content_format = self.get_content_format()
        if isinstance(content_format, FlatStructFormat) or hasattr(content_format, 'get_struct'):
            struct = content_format.get_struct()
        else:
            struct = None
        if struct is None:
            if self.is_accessible(verbose=verbose):
                struct = AUTO  # detect struct from source
        return self._get_native_struct(struct, save_if_not_yet=True, verbose=verbose)

    def set_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        struct = self._get_native_struct(struct, verbose=False)
        content_format = self.get_content_format()
        if inplace:
            if isinstance(content_format, (LeanFormat, ColumnarFormat)) or hasattr(content_format, 'set_struct'):
                content_format.set_struct(struct, inplace=True)
            else:
                raise TypeError('Cannot set struct for {}: method not supported (by design)'.format(content_format))
            if not self.get_initial_struct():
                self.set_initial_struct(struct, inplace=True)
        else:
            copy = self.make_new()
            assert isinstance(copy, ConnectorFormatMixin)
            copy.set_struct(struct, inplace=True)
            return self._assume_native(copy)

    def struct(self, struct: Struct) -> Native:
        struct = self._get_native_struct(struct).copy()
        self.set_struct(struct, inplace=True)
        return self._assume_native(self)

    def reset_struct_to_initial(self, verbose: bool = True, message: Optional[str] = None) -> Native:
        if not Auto.is_defined(message):
            message = f'in {repr(self)}'
        initial_struct = self.get_initial_struct()
        if verbose:
            for line in self.get_struct().get_struct_comparison_iter(initial_struct, message=message):
                self.log(f'Updated struct: {line}')
        return self.struct(initial_struct)

    def get_delimiter(self) -> str:
        content_format = self.get_content_format()
        if isinstance(content_format, ColumnarFormat) or hasattr(content_format, 'get_delimiter'):
            return content_format.get_delimiter()
        else:
            example_line = self.get_first_line() if self.is_existing() else ''
            return ColumnarFormat.detect_delimiter_by_example_line(example_line)

    def set_delimiter(self, delimiter: str, inplace: bool) -> Optional[Native]:
        content_format = self.get_content_format()
        if isinstance(content_format, ColumnarFormat) or hasattr(content_format, 'set_delimiter'):
            content_format = content_format.set_delimiter(delimiter, inplace=inplace)
            if not inplace:
                return self.set_content_format(content_format, inplace=inplace)
        elif isinstance(content_format, TextFormat):
            content_format = ColumnarFormat(delimiter=delimiter, **content_format.get_props())
            return self.set_content_format(content_format, inplace=inplace)

    def get_encoding(self) -> Optional[str]:
        content_format = self.get_content_format()
        if isinstance(content_format, TextFormat) or hasattr(content_format, 'get_encoding'):
            return content_format.get_encoding()

    def get_ending(self) -> str:
        content_format = self.get_content_format()
        if isinstance(content_format, TextFormat) or hasattr(content_format, 'get_ending'):
            return content_format.get_ending()
        else:
            return TextFormat().get_ending()

    def is_first_line_title(self) -> bool:
        content_format = self.get_content_format()
        if isinstance(content_format, ColumnarFormat) or hasattr(content_format, 'is_first_line_title'):
            return content_format.is_first_line_title()
        else:
            return False

    def get_title_row(self, close: bool = True) -> tuple:
        assert self.is_first_line_title(), 'For receive title row file/object must have first_line_is_title-flag'
        first_line = self.get_first_line(close=close)
        line_parser = fs.csv_loads(delimiter=self.get_delimiter())
        return line_parser(first_line)

    def get_detected_struct_by_title_row(
            self,
            set_struct: bool = False,  # deprecated argument
            types: Union[dict, Auto, None] = AUTO,
            verbose: AutoBool = AUTO,  # deprecated argument
    ) -> Struct:
        assert self.is_existing(), f'For detect struct file/object must be existing: {self.get_path()}'
        if not self.is_first_line_title():
            path = self.get_full_path()
            raise AssertionError(f'Can detect struct by title row only if first line is a title row ({path})')
        verbose = Auto.delayed_acquire(verbose, self.is_verbose)
        title_row = self.get_title_row(close=True)
        struct = self._get_struct_detected_by_title_row(title_row, types=types)
        message = 'Struct for {} detected by title row: {}'.format(self.get_name(), struct.get_struct_str(None))
        self.log(message, end='\n', verbose=verbose)
        if set_struct:
            self.set_struct(struct, inplace=True)
        return struct

    def _get_struct_from_source(self, types: Union[dict, Auto, None] = AUTO, verbose: bool = False):
        return self.get_detected_struct_by_title_row(types=types, verbose=verbose)

    def get_struct_from_source(
            self,
            set_struct: bool = False,
            use_declared_types: bool = True,
            skip_disconnected: bool = True,
            verbose: AutoBool = AUTO,
    ) -> Optional[Struct]:
        verbose = Auto.acquire(verbose, self.is_verbose())
        if skip_disconnected:
            if not self.is_accessible(verbose=verbose):
                return None
        else:
            assert self.is_accessible(), 'For detect struct storage must be connected: {}'.format(self.get_storage())
        if not self.is_existing():
            path = self.get_full_path() if hasattr(self, 'get_full_path') else self.get_path()
            raise FileNotFoundError(f'For detect struct file/object must be existing: {path}')
        declared_types = dict()
        if use_declared_types:
            declared_format = self.get_declared_format()
            if isinstance(declared_format, FlatStructFormat) or hasattr(declared_format, 'get_struct'):
                declared_struct = declared_format.get_struct()
                if isinstance(declared_struct, StructInterface) or hasattr(declared_struct, 'get_types_dict'):
                    declared_types = declared_struct.get_types_dict()
        struct = self._get_struct_from_source(types=declared_types, verbose=verbose)
        message = 'Struct for {} detected from source: {}'.format(self.get_name(), struct.get_struct_str(None))
        self.log(message, end='\n', verbose=verbose)
        if set_struct:
            self.set_struct(struct, inplace=True)
        return struct

    def is_gzip(self) -> bool:
        content_format = self.get_content_format()
        if isinstance(content_format, ParsedFormat):
            return content_format.is_gzip()
        return False

    def get_stream_type(self) -> StreamType:
        content_format = self.get_content_format()
        return content_format.get_default_stream_type()

    def get_item_type(self) -> ItemType:
        stream_class = self.get_stream_type().get_class()
        if hasattr(stream_class, 'get_item_type'):
            return stream_class.get_item_type()
        else:
            stream_obj = stream_class([])
        if hasattr(stream_obj, 'get_item_type'):
            return stream_obj.get_item_type()
        else:
            return ItemType.Any

    def get_content_type(self) -> ContentType:
        return self.get_content_format().get_content_type()

    @staticmethod
    def _assume_native(connector) -> Native:
        return connector
