from typing import Optional, Union, Iterable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from interfaces import StructInterface, FieldInterface, DialectType, FieldType, Field, Name, Array, ARRAY_TYPES
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..utils.decorators import deprecated_with_alternative
    from ..interfaces import StructInterface, FieldInterface, DialectType, FieldType, Field, Name, Array, ARRAY_TYPES


class LegacyStruct(StructInterface):
    FieldClass = None

    @deprecated_with_alternative('fields.FieldGroup')
    def __init__(self, fields_descriptions: Iterable):
        self._import_workaround()
        self.fields_descriptions = list()
        for field in fields_descriptions:
            self.append_field(field)

    def _import_workaround(self):
        # Temporary workaround for cyclic import dependencies
        try:  # Assume we're a sub-module in a package.
            from fields.legacy_field import LegacyField
        except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
            from ..fields.legacy_field import LegacyField
        self.FieldClass = LegacyField

    def append_field(self, field, default_type=None, before=False, inplace=True):
        field_class = self.FieldClass
        if isinstance(field, FieldInterface):
            field_desc = field
        elif isinstance(field, str):
            field_desc = field_class(field, default_type)
        elif isinstance(field, ARRAY_TYPES):
            field_desc = field_class(*field)
        elif isinstance(field, dict):
            field_desc = field_class(**field)
        else:
            raise TypeError('Expected Field or str, got {} as {}'.format(field, type(field)))
        if before:
            fields = [field_desc] + self.fields_descriptions
        else:
            fields = self.fields_descriptions + [field_desc]
        if inplace:
            self.fields_descriptions = fields
        else:
            return LegacyStruct(fields)

    def append(self, field_or_group, default_type=None, inplace=None):
        if isinstance(field_or_group, StructInterface):
            return self.add_fields(field_or_group.get_fields_descriptions(), default_type=default_type, inplace=inplace)
        elif isinstance(field_or_group, Iterable):
            return self.add_fields(field_or_group, default_type=default_type, inplace=inplace)
        else:
            return self.append_field(field_or_group, default_type=default_type, inplace=inplace)

    def remove_fields(self, *fields, inplace: bool = True):
        removing_fields = arg.update(fields)
        existing_fields = self.get_fields_descriptions()
        if inplace:
            for f in existing_fields.copy():
                if isinstance(f, ARRAY_TYPES):
                    name = f[0]
                elif hasattr(f, 'get_name'):
                    name = f.get_name()
                else:
                    name = f
                if name in removing_fields:
                    existing_fields.remove(f)
        else:
            raise NotImplementedError

    def add_fields(self, *fields, default_type=None, inplace=False):
        if inplace:
            for f in fields:
                self.append_field(f, default_type=default_type, inplace=True)
        else:
            return LegacyStruct(self.get_fields_descriptions() + list(fields))

    def get_fields(self):
        return self.fields_descriptions

    @staticmethod
    def _detect_field_type_by_name(field_name) -> FieldType:
        field_type = FieldType.detect_by_name(field_name)
        assert isinstance(field_type, FieldType)
        return field_type

    @classmethod
    def detect_struct_by_title_row(cls, title_row: Iterable) -> StructInterface:
        field_class = cls.FieldClass
        struct = LegacyStruct([])
        for name in title_row:
            field_type = cls._detect_field_type_by_name(name)
            struct.append_field(
                field_class(name, field_type)
            )
        return struct

    @classmethod
    def detect_schema_by_title_row(cls, title_row: Iterable) -> StructInterface:
        return cls.detect_struct_by_title_row(title_row=title_row)

    def get_fields_count(self):
        return len(self.fields_descriptions)

    def get_struct_str(self, dialect: DialectType = DialectType.String) -> str:
        if not isinstance(dialect, DialectType):
            dialect = DialectType.detect(dialect)
        template = '{}: {}' if dialect in ('str', 'py') else '{} {}'
        field_strings = [template.format(c.get_name(), c.get_type_in(dialect)) for c in self.get_fields()]
        return ', '.join(field_strings)

    def get_schema_str(self, dialect: DialectType = DialectType.Python):
        return self.get_struct_str(dialect=dialect)

    def __repr__(self):
        return '[{}]'.format(self.get_struct_str())

    def __str__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.get_struct_str())

    def get_column_count(self) -> int:
        return len(self.get_columns())

    def get_columns(self):
        return [c.get_name() for c in self.fields_descriptions]

    def get_types_list(self, dialect: Optional[DialectType] = DialectType.String) -> list:
        if arg.is_defined(dialect):
            return [f.get_type_in(dialect) for f in self.get_fields()]
        else:
            return [f.get_type() for f in self.get_fields()]

    def get_types_dict(self, dialect: Union[DialectType, arg.Auto, None] = arg.AUTO) -> dict:
        names = map(lambda f: arg.get_name(f), self.get_fields())
        types = self.get_types_list(dialect)
        return dict(zip(names, types))

    def get_types(self, dialect: DialectType = DialectType.String, as_list: bool = True) -> Union[list, dict]:
        if as_list:
            return self.get_types_list(dialect)
        else:
            return self.get_types_dict(dialect)

    def set_types(self, dict_field_types=None, inplace=False, **kwargs):
        for field_name, field_type in list((dict_field_types or {}).items()) + list(kwargs.items()):
            field_description = self.get_field_description(field_name)
            assert isinstance(field_description, FieldInterface)
            canonic_type = FieldType.get_canonic_type(field_type)
            assert isinstance(canonic_type, FieldType)
            field_description.set_type(canonic_type, inplace=True)
        if not inplace:
            return self

    def get_field_position(self, field: Name, skip_errors: bool = False) -> Optional[int]:
        if isinstance(field, int):
            if field < self.get_fields_count():
                return field
        elif isinstance(field, str):
            try:
                return self.get_columns().index(field)
            except ValueError:
                pass
            except IndexError:
                pass
            if not skip_errors:
                raise IndexError('There is no field {} in struct: {}'.format(field, self.get_struct_str()))

    def get_fields_positions(self, names: Array):
        columns = self.get_columns()
        return [columns.index(f) for f in names]

    def get_converters(self, src='str', dst='py'):
        converters = list()
        for desc in self.fields_descriptions:
            converters.append(desc.get_converter(src, dst))
        return tuple(converters)

    def get_field_description(self, field_name: Name) -> FieldInterface:
        field_position = self.get_field_position(field_name)
        return self.get_fields_descriptions()[field_position]

    def get_fields_descriptions(self):
        return self.fields_descriptions

    def is_valid_row(self, row):
        for value, field_type in zip(row, self.get_types('py')):
            if not isinstance(value, field_type):
                if not (field_type in (int, float) and not value):  # 0 is not float
                    return False
        return True

    def copy(self):
        return LegacyStruct(self.fields_descriptions)

    def simple_select_fields(self, fields: Array):
        return LegacyStruct(
            [self.get_field_description(f) for f in fields]
        )

    def __iter__(self):
        yield from self.get_fields_descriptions()

    def __add__(self, other: Union[FieldInterface, StructInterface, str]) -> StructInterface:
        if isinstance(other, (str, int, FieldInterface)):
            return self.append_field(other, inplace=False)
        elif isinstance(other, (StructInterface, Iterable)):
            return self.append(other, inplace=False).set_name(None, inplace=False)
        else:
            raise TypeError('Expected other as field or struct, got {} as {}'.format(other, type(other)))


SchemaDescription = LegacyStruct
