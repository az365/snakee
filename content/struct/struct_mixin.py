from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, Type, Callable

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from interfaces import (
        StructInterface, StructMixinInterface,
        ItemType, DialectType, FieldType, Field, FieldName, FieldNo,
        AUTO, Auto, AutoBool, Row, Array, ARRAY_TYPES,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        StructInterface, StructMixinInterface,
        ItemType, DialectType, FieldType, Field, FieldName, FieldNo,
        AUTO, Auto, AutoBool, Row, Array, ARRAY_TYPES,
    )

Struct = Optional[StructInterface]


class StructMixin(StructMixinInterface, ABC):
    @abstractmethod
    def get_struct(self):
        pass

    def get_struct_str(self, dialect: DialectType = DialectType.Postgres) -> str:
        return self.get_struct().get_struct_str(dialect=dialect)

    def add_fields(self, *fields, default_type: Type = None, inplace: bool = False):
        self.get_struct().add_fields(*fields, default_type=default_type, inplace=True)
        if not inplace:
            return self

    def remove_fields(self, *fields, inplace=True):
        self.get_struct().remove_fields(*fields, inplace=True)
        if not inplace:
            return self

    def get_columns(self) -> Row:
        return self.get_struct().get_columns()

    def get_column_count(self) -> int:
        return len(self.get_columns())

    def get_types_list(self, dialect: DialectType = DialectType.String) -> list:
        return self.get_struct().get_types_list(dialect)

    def get_types_dict(self, dialect: Union[DialectType, arg.Auto, None] = AUTO) -> dict:
        return self.get_struct().get_types_dict(dialect)

    def get_types(self, dialect: DialectType = DialectType.String, as_list: bool = True) -> Iterable:
        return self.get_struct().get_types(dialect, as_list=as_list)

    def set_types(self, dict_field_types: Optional[dict] = None, inplace: bool = False, **kwargs):
        self.get_struct().set_types(dict_field_types=dict_field_types, inplace=True, **kwargs)
        if not inplace:
            return self

    def get_field_position(self, field: Field) -> Optional[FieldNo]:
        if isinstance(field, FieldNo):
            return field
        else:  # isinstance(field, FieldName)
            field_name = arg.get_name(field)
            return self.get_struct().get_field_position(field_name)

    def get_fields_positions(self, fields: Row) -> Row:
        return [self.get_field_position(f) for f in fields]

    @classmethod
    def _get_struct_detected_by_title_row(cls, title_row: Iterable, types: Optional[dict] = None) -> StructInterface:
        struct_class = cls._get_struct_class()
        if not arg.is_defined(types):
            types = dict()
        detected_struct = struct_class([])
        for name in title_row:
            if name in types:
                field_type = FieldType.convert(types[name])
            else:
                field_type = FieldType.detect_by_name(name)
            detected_struct.append_field(name, default_type=field_type, exclude_duplicates=False, inplace=True)
        return detected_struct

    @staticmethod
    def _get_struct_class() -> Union[Type, Callable, StructInterface]:
        struct_row_class = ItemType.StructRow.get_class()
        flat_struct_class = struct_row_class([], []).get_struct().__class__
        return flat_struct_class

    def _get_native_struct(self, raw_struct: Struct, verbose: AutoBool = AUTO) -> Struct:
        if hasattr(self, 'is_verbose') and not arg.is_defined(verbose):
            verbose = self.is_verbose()
        if raw_struct is None:
            native_struct = None
        elif isinstance(raw_struct, StructInterface):
            native_struct = raw_struct
        elif hasattr(raw_struct, 'get_fields'):
            struct_class = self._get_struct_class()
            native_struct = struct_class(raw_struct)
        elif isinstance(raw_struct, ARRAY_TYPES):
            if verbose:
                msg = 'Struct as list is deprecated, use FlatStruct(StructInterface) class instead'
                if hasattr(self, 'log'):
                    self.log(msg=msg, level=30)
                else:
                    print(msg)
            column_names = raw_struct
            has_types_descriptions = [isinstance(f, ARRAY_TYPES) for f in raw_struct]
            if max(has_types_descriptions):
                struct_class = self._get_struct_class()
                native_struct = struct_class(raw_struct)
            else:
                native_struct = self._get_struct_detected_by_title_row(column_names)
        elif raw_struct == AUTO:
            native_struct = None
            if hasattr(self, 'is_first_line_title'):
                if self.is_first_line_title():
                    if hasattr(self, 'get_detected_struct_by_title_row'):
                        native_struct = self.get_detected_struct_by_title_row(set_struct=False, verbose=verbose)
                    elif hasattr(self, 'get_title_row'):
                        title_row = self.get_title_row(close=True)
                        native_struct = self._get_struct_detected_by_title_row(title_row)
        else:
            message = 'struct must be FlatStruct(StructInterface), got {}'.format(type(raw_struct))
            raise TypeError(message)
        return native_struct
