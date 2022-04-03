from abc import ABC
from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.functions.arguments import get_name, get_value
    from base.abstract.simple_data import SimpleDataWrapper, EMPTY
    from base.mixin.data_mixin import MultiMapDataMixin
    from interfaces import FieldInterface, StructInterface, FieldType, DialectType, ARRAY_TYPES
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import Auto, AUTO
    from ...base.functions.arguments import get_name, get_value
    from ...base.abstract.simple_data import SimpleDataWrapper, EMPTY
    from ...base.mixin.data_mixin import MultiMapDataMixin
    from ...interfaces import FieldInterface, StructInterface, FieldType, DialectType, ARRAY_TYPES

Native = Union[SimpleDataWrapper, FieldInterface]


class AbstractField(SimpleDataWrapper, MultiMapDataMixin, FieldInterface, ABC):
    _struct_builder: Optional[Callable] = None

    def __init__(self, name: str, field_type: FieldType = FieldType.Any, caption: str = EMPTY, properties=None):
        field_type = Auto.delayed_acquire(field_type, FieldType.detect_by_name, field_name=name)
        field_type = FieldType.get_canonic_type(field_type, ignore_missing=True)
        assert isinstance(field_type, FieldType), 'Expected FieldType, got {}'.format(field_type)
        self._type = field_type
        super().__init__(name=name, caption=caption, data=properties)

    def set_type(self, field_type: FieldType, inplace: bool) -> Native:
        if inplace:
            self._type = field_type
            return self
        else:
            field = self.set_outplace(field_type=field_type)
            return self._assume_native(field)

    def get_type(self) -> FieldType:
        return self._type

    def get_type_name(self) -> str:
        type_name = get_value(self.get_type())
        if not isinstance(type_name, str):
            type_name = get_name(type_name)
        return str(type_name)

    def get_type_in(self, dialect: DialectType):
        if not isinstance(dialect, DialectType):
            dialect = DialectType.detect(dialect)
        if dialect == DialectType.String:
            return self.get_type_name()
        else:
            return self.get_type().get_type_in(dialect)

    def get_converter(self, source: DialectType, target: DialectType) -> Callable:
        return self.get_type().get_converter(source, target)

    @classmethod
    def set_struct_builder(cls, struct_builder: Callable):
        cls._struct_builder = struct_builder

    @classmethod
    def get_struct_builder(cls, default: Callable = list) -> Callable:
        if cls._struct_builder:
            return cls._struct_builder
        else:
            return default

    def get_sql_expression(self) -> str:
        return self.get_name()

    def get_str_repr(self) -> str:
        return self.get_name()

    def get_brief_repr(self) -> str:
        return '{}: {}'.format(self.get_name(), self.get_type_name())

    def __str__(self):
        return self.get_detailed_repr()

    def __add__(self, other: Union[FieldInterface, StructInterface, str]) -> StructInterface:
        struct_builder = self.get_struct_builder()
        field_builder = self.__class__
        if isinstance(other, str):
            return struct_builder([self, field_builder(other)])
        elif isinstance(other, AbstractField):
            return struct_builder([self, other])
        elif isinstance(other, ARRAY_TYPES):
            return struct_builder([self] + list(other))
        elif isinstance(other, StructInterface):
            struct = other.append_field(self, before=True, inplace=False)
            assert isinstance(struct, StructInterface), struct
            return struct
        else:
            raise TypeError('Expected other as field or struct, got {} as {}'.format(other, type(other)))

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
