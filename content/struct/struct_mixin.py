from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, Type, Callable

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructInterface, StructMixinInterface,
        DialectType, ValueType, Field, FieldName, FieldNo,
        Links, Array, ARRAY_TYPES,
    )
    from base.functions.arguments import get_name
    from base.functions.errors import get_type_err_msg
    from content.struct.struct_classes import StructType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructInterface, StructMixinInterface,
        DialectType, ValueType, Field, FieldName, FieldNo,
        Links, Array, ARRAY_TYPES,
    )
    from ...base.functions.arguments import get_name
    from ...base.functions.errors import get_type_err_msg
    from .struct_classes import StructType

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

    def get_columns(self) -> Array:
        struct = self.get_struct()
        if struct is not None:
            return struct.get_columns()
        else:
            return None

    def get_column_count(self) -> int:
        columns = self.get_columns()
        if columns is not None:
            return len(columns)
        else:
            return None

    def get_types_list(self, dialect: DialectType = DialectType.String) -> list:
        return self.get_struct().get_types_list(dialect)

    def get_types_dict(self, dialect: Optional[DialectType] = None) -> dict:
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
            field_name = get_name(field)
            return self.get_struct().get_field_position(field_name)

    def get_fields_positions(self, fields: Array) -> Array:
        return [self.get_field_position(f) for f in fields]

    @classmethod
    def _get_struct_detected_by_title_row(
            cls,
            title_row: Iterable,
            types: Links = None,
    ) -> StructInterface:
        struct_class = cls._get_struct_class()
        if types is None:
            types = dict()
        detected_struct = struct_class([])
        for name in title_row:
            if name in types:
                field_type = ValueType.convert(types[name])
            else:
                field_type = ValueType.detect_by_name(name)
            detected_struct.append_field(name, default_type=field_type, exclude_duplicates=False, inplace=True)
        return detected_struct

    @staticmethod
    def _get_struct_class() -> Union[Type, Callable, StructInterface]:
        return StructType.get_default().get_class()

    def _get_native_struct(self, raw_struct: Struct, save_if_not_yet: bool = False, verbose: Optional[bool] = None) -> Struct:
        if hasattr(self, 'is_verbose') and verbose is None:
            verbose = self.is_verbose()
        if isinstance(raw_struct, StructInterface):
            native_struct = raw_struct
        elif hasattr(raw_struct, 'get_fields'):
            struct_class = self._get_struct_class()
            native_struct = struct_class(raw_struct)
        elif isinstance(raw_struct, ARRAY_TYPES):
            if verbose:
                msg = 'Struct as list is deprecated, use FlatStruct(StructInterface) class instead'
                if hasattr(self, 'get_logger'):
                    logger = self.get_logger()
                    logger.warning(msg, category=DeprecationWarning, stacklevel=2)
                elif hasattr(self, 'log'):
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
        elif raw_struct is None:
            native_struct = None
            if hasattr(self, 'get_struct_from_source'):
                native_struct = self.get_struct_from_source(
                    set_struct=save_if_not_yet,
                    skip_missing=True,
                    verbose=verbose,
                )
            elif hasattr(self, 'is_first_line_title'):
                if self.is_first_line_title():
                    if hasattr(self, 'get_detected_struct_by_title_row'):
                        native_struct = self.get_detected_struct_by_title_row(
                            set_struct=save_if_not_yet, verbose=verbose,
                        )
                    elif hasattr(self, 'get_title_row'):
                        title_row = self.get_title_row(close=True)
                        native_struct = self._get_struct_detected_by_title_row(title_row)
        else:
            raise TypeError(get_type_err_msg(got=raw_struct, expected='FlatStruct'))
        return native_struct
