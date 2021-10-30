from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, Type, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        LeafConnectorInterface, StructInterface, StructFileInterface, IterableStreamInterface,
        ItemType, FieldType,
        AUTO, Auto, AutoBool, Array, ARRAY_TYPES,
    )
    from connectors.content_format.text_format import AbstractFormat, ParsedFormat, TextFormat
    from connectors.content_format.lean_format import LeanFormat, ColumnarFormat, FlatStructFormat
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        LeafConnectorInterface, StructInterface, StructFileInterface, IterableStreamInterface,
        ItemType, FieldType,
        AUTO, Auto, AutoBool, Array, ARRAY_TYPES,
    )
    from ..content_format.text_format import AbstractFormat, ParsedFormat, TextFormat
    from ..content_format.lean_format import LeanFormat, ColumnarFormat, FlatStructFormat

Native = Union[LeafConnectorInterface, IterableStreamInterface, StructFileInterface]
Struct = Optional[StructInterface]


class ConnectorFormatMixin(LeafConnectorInterface, ABC):
    def get_initial_struct(self) -> Struct:
        initial_format = self.get_initial_format()
        if isinstance(initial_format, FlatStructFormat) or hasattr(initial_format, 'get_struct'):
            return initial_format.get_struct()

    def set_initial_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        struct = self._get_native_struct(struct).copy()
        initial_format = self.get_initial_format()
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

    def get_struct(self) -> Struct:
        content_format = self.get_content_format()
        if isinstance(content_format, FlatStructFormat) or hasattr(content_format, 'get_struct'):
            struct = content_format.get_struct()
        else:
            struct = None
        return self._get_native_struct(struct)

    def set_struct(self, struct: Struct, inplace: bool) -> Optional[Native]:
        struct = self._get_native_struct(struct)
        content_format = self.get_content_format()
        if inplace:
            if isinstance(content_format, (LeanFormat, ColumnarFormat)) or hasattr(content_format, 'set_struct'):
                content_format.set_struct(struct, inplace=True)
            else:
                raise TypeError('Cannot set struct for {}: method not supported (by design)'.format(content_format))
            if not self.get_initial_struct():
                self.set_initial_struct(struct, inplace=True)
        else:
            # return self.make_new(struct=struct)
            copy = self.make_new()
            assert isinstance(copy, ConnectorFormatMixin)
            copy.set_struct(struct, inplace=True)
            return self._assume_native(copy)

    def struct(self, struct: Struct) -> Native:
        struct = self._get_native_struct(struct).copy()
        self.set_struct(struct, inplace=True)
        return self._assume_native(self)

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

    def is_first_line_title(self) -> bool:
        content_format = self.get_content_format()
        if isinstance(content_format, ColumnarFormat) or hasattr(content_format, 'is_first_line_title'):
            return content_format.is_first_line_title()
        else:
            return False

    def get_title_row(self, close: bool = True) -> tuple:
        assert self.is_first_line_title(), 'For receive title row file/object must have first_line_is_title-flag'
        first_line = self.get_first_line(close=close)
        content_format = self.get_content_format()
        if isinstance(content_format, ColumnarFormat):
            title_row = content_format.get_parsed_line(first_line, item_type=ItemType.Row)
        else:
            delimiter = ColumnarFormat.detect_delimiter_by_example_line(first_line)
            title_row = ColumnarFormat(False, delimiter).get_parsed_line(first_line, item_type=ItemType.Row)
        return title_row

    def get_detected_struct_by_title_row(self, set_struct: bool = False, verbose: AutoBool = AUTO) -> Struct:
        assert self.is_first_line_title(), 'Can detect struct by title row only if first line is a title row'
        assert self.is_existing(), 'For detect struct by title row file/object must be existing'
        verbose = arg.acquire(verbose, self.is_verbose())
        title_row = self.get_title_row(close=True)
        struct = self._get_struct_detected_by_title_row(title_row)
        message = 'Struct for {} detected by title row: {}'.format(self.get_name(), struct.get_struct_str(None))
        self.log(message, end='\n', verbose=verbose)
        if set_struct:
            self.set_struct(struct, inplace=True)
        return struct

    @classmethod
    def _get_struct_detected_by_title_row(cls, title_row: Iterable) -> StructInterface:
        struct_class = cls._get_struct_class()
        detected_struct = struct_class([])
        for name in title_row:
            field_type = FieldType.detect_by_name(name)
            detected_struct.append_field(name, default_type=field_type, inplace=True)
        return detected_struct

    @staticmethod
    def _get_struct_class() -> Union[Type, Callable, StructInterface]:
        struct_row_class = ItemType.StructRow.get_class()
        flat_struct_class = struct_row_class([], []).get_struct().__class__
        return flat_struct_class

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj

    def _get_native_struct(self, raw_struct: Struct, verbose: AutoBool = AUTO) -> Struct:
        verbose = arg.delayed_undefault(verbose, self.is_verbose)
        if raw_struct is None:
            native_struct = None
        elif isinstance(raw_struct, StructInterface):
            native_struct = raw_struct
        elif hasattr(raw_struct, 'get_fields'):
            native_struct = self._get_native_struct(raw_struct)
        elif isinstance(raw_struct, ARRAY_TYPES):
            if verbose:
                msg = 'Struct as list is deprecated, use FlatStruct(StructInterface) class instead'
                self.log(msg, level=30)
            column_names = raw_struct
            has_types_descriptions = [isinstance(f, ARRAY_TYPES) for f in raw_struct]
            if max(has_types_descriptions):
                native_struct = self._get_native_struct(raw_struct)
            else:
                native_struct = self._get_struct_detected_by_title_row(column_names)
        elif raw_struct == AUTO:
            if self.is_first_line_title():
                native_struct = self.get_detected_struct_by_title_row(set_struct=False, verbose=verbose)
            else:
                native_struct = None
        else:
            message = 'struct must be FlatStruct(StructInterface), got {}'.format(type(raw_struct))
            raise TypeError(message)
        return native_struct

    def is_gzip(self) -> bool:
        content_format = self.get_content_format()
        if isinstance(content_format, ParsedFormat):
            return content_format.is_gzip()
        return False

    @abstractmethod
    def is_verbose(self) -> bool:
        pass

    @abstractmethod
    def make_new(self, *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def get_first_line(self, close: bool = True) -> str:
        pass

    @abstractmethod
    def get_content_format(self) -> AbstractFormat:
        pass

    @abstractmethod
    def set_content_format(self, content_format: AbstractFormat, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def get_initial_format(self) -> AbstractFormat:
        pass
