from typing import Optional, Union, Iterable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract.simple_data import SimpleDataWrapper
    from fields.field_type import FieldType
    from fields.field_interface import FieldInterface
    from fields.advanced_field import AdvancedField
    from selection.abstract_expression import AbstractDescription
    from connectors.databases import dialect as di
    from fields.schema_interface import SchemaInterface
    from items.struct_row_interface import StructRowInterface
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base.abstract.simple_data import SimpleDataWrapper
    from .field_type import FieldType
    from .field_interface import FieldInterface
    from .advanced_field import AdvancedField
    from ..selection.abstract_expression import AbstractDescription
    from ..connectors.databases import dialect as di
    from .schema_interface import SchemaInterface
    from ..items.struct_row_interface import StructRowInterface
    from ..loggers.selection_logger_interface import SelectionLoggerInterface

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
FieldProps = dict
Field = Union[FieldID, FieldProps, FieldInterface]
Type = Union[FieldType, arg.DefaultArgument]
Group = Union[SchemaInterface, Iterable]
Array = Union[list, tuple]
ARRAY_SUBTYPES = list, tuple

META_MEMBER_MAPPING = dict(_data='fields')


class FieldGroup(SimpleDataWrapper, SchemaInterface):
    def __init__(self, fields: Iterable, name: Optional[str] = None, default_type: Type = arg.DEFAULT):
        name = arg.undefault(name, arg.get_generated_name(prefix='FieldGroup'))
        super().__init__(name=name, data=list())
        for field in fields:
            if isinstance(field, SchemaInterface):
                self.add_fields(field.get_fields_descriptions(), default_type=default_type, inplace=True)
            else:
                self.append_field(field, default_type=default_type, inplace=True)

    def get_fields(self) -> list:
        return list(self.get_data())

    def set_fields(self, fields: Iterable, inplace: bool) -> Optional[SchemaInterface]:
        return self.set_data(data=fields, inplace=inplace, reset_dynamic_meta=False)

    def fields(self, fields: Iterable) -> SchemaInterface:
        self._data = list(fields)
        return self

    @staticmethod
    def _is_field(field):
        return hasattr(field, 'get_name') and hasattr(field, 'get_type')

    def append_field(
            self,
            field: Field,
            default_type: FieldType = FieldType.Any,
            before: bool = False,
            inplace: bool = True,
    ) -> Optional[SchemaInterface]:
        if self._is_field(field):
            field_desc = field
        elif isinstance(field, str):
            field_desc = AdvancedField(field, default_type)
        elif isinstance(field, ARRAY_SUBTYPES):
            field_desc = AdvancedField(*field)
        elif isinstance(field, dict):
            field_desc = AdvancedField(**field)
        else:
            raise TypeError('Expected field, str or dict, got {} as {}'.format(field, type(field)))
        if before:
            fields = [field_desc] + self.get_fields()
        else:
            fields = self.get_fields() + [field_desc]
        return self.set_fields(fields, inplace=inplace)

    def append(self, field_or_group: Union[Field, Group], default_type=None, inplace=None) -> Optional[SchemaInterface]:
        if isinstance(field_or_group, SchemaInterface):
            return self.add_fields(field_or_group.get_fields_descriptions(), default_type=default_type, inplace=inplace)
        elif isinstance(field_or_group, Iterable):
            return self.add_fields(field_or_group, default_type=default_type, inplace=inplace)
        else:
            return self.append_field(field_or_group, default_type=default_type, inplace=inplace)

    def add_fields(self, *fields, default_type=None, inplace=False, name=None) -> Optional[SchemaInterface]:
        fields = arg.update(fields)
        if inplace:
            for f in fields:
                self.append(f, default_type=default_type, inplace=True)
        else:
            return FieldGroup(self.get_fields_descriptions() + list(fields), name=name)

    def get_fields_count(self) -> int:
        return len(self.get_fields())

    def get_schema_str(self, dialect='py') -> str:
        if dialect is not None and dialect not in di.DIALECTS:
            dialect = di.get_dialect_for_connector(dialect)
        template = '{}: {}' if dialect in ('str', 'py') else '{} {}'
        field_strings = [template.format(c.get_name(), c.get_type_in(dialect)) for c in self.get_fields()]
        return ', '.join(field_strings)

    def get_columns(self) -> list:
        return [c.get_name() for c in self.get_fields()]

    def get_types(self, dialect) -> list:
        return [c.get_type_in(dialect) for c in self.get_fields()]

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return META_MEMBER_MAPPING

    def set_types(self, dict_field_types: Optional[dict] = None, inplace=True, **kwargs) -> Optional[SchemaInterface]:
        if inplace:
            self.types(dict_field_types=dict_field_types, **kwargs)
        else:
            copy = self.copy().types(dict_field_types=dict_field_types, **kwargs)
            return copy

    def types(self, dict_field_types: Optional[dict] = None, **kwargs) -> SchemaInterface:
        for field_name, field_type in list((dict_field_types or {}).items()) + list(kwargs.items()):
            field = self.get_field_description(field_name)
            assert hasattr(field, 'set_type'), 'Expected SimpleField or FieldDescription, got {}'.format(field)
            field.set_type(FieldType.detect_by_type(field_type), inplace=True)
        return self

    def common_type(self, field_type: Union[FieldType, type]) -> SchemaInterface:
        for f in self.get_fields_descriptions():
            assert isinstance(f, FieldInterface)
            f.set_type(field_type, inplace=True)
        return self

    def get_field_position(self, field: FieldID) -> Optional[FieldNo]:
        if isinstance(field, FieldNo):
            if field < self.get_fields_count():
                return field
        elif isinstance(field, FieldName):
            try:
                return self.get_columns().index(field)
            except ValueError or IndexError:
                return None
        elif isinstance(field, FieldInterface):
            return self.get_field_position(field.get_name())

    def get_fields_positions(self, names: Iterable) -> list:
        columns = self.get_columns()
        return [columns.index(f) for f in names]

    def get_converters(self, src='str', dst='py') -> tuple:
        converters = list()
        for desc in self.get_fields():
            converters.append(desc.get_converter(src, dst))
        return tuple(converters)

    def get_field_description(self, field_name) -> Union[FieldInterface, AdvancedField]:
        field_position = self.get_field_position(field_name)
        assert field_position is not None, 'Field {} not found (existing fields: {})'.format(
            field_name, self.get_columns(),
        )
        return self.get_fields()[field_position]

    def get_fields_descriptions(self) -> list:
        return self.get_fields()

    def is_valid_row(self, row: Union[Iterable, StructRowInterface]) -> bool:
        for value, field_type in zip(row, self.get_types('py')):
            if not isinstance(value, field_type):
                return False
        return True

    def copy(self):
        return FieldGroup(name=self.get_name(), fields=self.get_fields())

    def simple_select_fields(self, fields: Iterable):
        return FieldGroup(
            [self.get_field_description(f) for f in fields]
        )

    def __repr__(self):
        return self.get_schema_str(None)

    def __str__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.get_schema_str('str'))

    def __iter__(self):
        yield from self.get_fields_descriptions()

    def __getitem__(self, item: Union[int, str, slice]):
        if isinstance(item, slice):
            return FieldGroup(self.get_fields_descriptions()[item])
        elif isinstance(item, int):
            return self.get_fields_descriptions()[item]
        else:  # elif isinstance(item, str):
            for f in self.get_fields_descriptions():
                if f.get_name() == item:
                    return f
            raise ValueError('Field with name {} not found (in group {})'.format(item, self))

    def __add__(self, other: Union[FieldInterface, SchemaInterface, str]) -> SchemaInterface:
        if isinstance(other, (str, int, FieldInterface)):
            return self.append_field(other, inplace=False)
        elif isinstance(other, (SchemaInterface, Iterable)):
            return self.append(other, inplace=False).set_name(None, inplace=False)
        else:
            raise TypeError('Expected other as field or schema, got {} as {}'.format(other, type(other)))
