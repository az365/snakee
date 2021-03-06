from typing import Optional, Union, Iterable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from fields.field_interface import FieldInterface
    from fields.schema_interface import SchemaInterface
    from fields.field_type import get_canonic_type, HEURISTIC_SUFFIX_TO_TYPE
    from connectors.databases import dialect as di
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..fields.field_interface import FieldInterface
    from ..fields.schema_interface import SchemaInterface
    from ..fields.field_type import get_canonic_type, HEURISTIC_SUFFIX_TO_TYPE
    from ..connectors.databases import dialect as di

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Array = Union[list, tuple]
ARRAY_SUBTYPES = list, tuple


class SchemaDescription(SchemaInterface):
    FieldDescription = None

    def __init__(self, fields_descriptions: Iterable):
        self._import_workaround()
        self.fields_descriptions = list()
        for field in fields_descriptions:
            self.append_field(field)

    def _import_workaround(self):
        # Temporary workaround for cyclic import dependencies
        try:  # Assume we're a sub-module in a package.
            from schema.field_description import FieldDescription
        except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
            from .field_description import FieldDescription
        self.FieldDescription = FieldDescription

    def append_field(self, field, default_type=None, before=False, inplace=True):
        FieldDescription = self.FieldDescription
        if isinstance(field, FieldInterface):
            field_desc = field
        elif isinstance(field, str):
            field_desc = FieldDescription(field, default_type)
        elif isinstance(field, ARRAY_SUBTYPES):
            field_desc = FieldDescription(*field)
        elif isinstance(field, dict):
            field_desc = FieldDescription(**field)
        else:
            raise TypeError('Expected Field or str, got {} as {}'.format(field, type(field)))
        if before:
            fields = [field_desc] + self.fields_descriptions
        else:
            fields = self.fields_descriptions + [field_desc]
        if inplace:
            self.fields_descriptions = fields
        else:
            return SchemaDescription(fields)

    def append(self, field_or_group, default_type=None, inplace=None):
        if isinstance(field_or_group, SchemaInterface):
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
                if isinstance(f, ARRAY_SUBTYPES):
                    name = f[0]
                elif hasattr(f, 'get_name'):
                    name = f.get_name()
                else:
                    name = f
                if name in removing_fields:
                    existing_fields.remove()
        else:
            raise NotImplementedError

    def add_fields(self, *fields, default_type=None, inplace=False):
        if inplace:
            for f in fields:
                self.append_field(f, default_type=default_type, inplace=True)
        else:
            return SchemaDescription(self.get_fields_descriptions() + list(fields))

    def get_fields(self):
        return self.fields_descriptions

    @staticmethod
    def _detect_field_type_by_name(field_name):
        name_parts = field_name.split('_')
        default_type = HEURISTIC_SUFFIX_TO_TYPE[None]
        field_type = default_type
        for suffix in HEURISTIC_SUFFIX_TO_TYPE:
            if suffix in name_parts:
                field_type = HEURISTIC_SUFFIX_TO_TYPE[suffix]
                break
        return field_type

    @classmethod
    def detect_schema_by_title_row(cls, title_row: Iterable) -> SchemaInterface:
        FieldDescription = cls.FieldDescription
        schema = SchemaDescription([])
        for name in title_row:
            field_type = cls._detect_field_type_by_name(name)
            schema.append_field(
                FieldDescription(name, field_type)
            )
        return schema

    def get_fields_count(self):
        return len(self.fields_descriptions)

    def get_schema_str(self, dialect='py'):
        if dialect is not None and dialect not in di.DIALECTS:
            dialect = di.get_dialect_for_connector(dialect)
        template = '{}: {}' if dialect in ('str', 'py') else '{} {}'
        field_strings = [template.format(c.get_name(), c.get_type_in(dialect)) for c in self.get_fields()]
        return ', '.join(field_strings)

    def __repr__(self):
        return '[{}]'.format(self.get_schema_str())

    def __str__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.get_schema_str())

    def get_columns(self):
        return [c.get_name() for c in self.fields_descriptions]

    def get_types(self, dialect):
        return [c.get_type_in(dialect) for c in self.fields_descriptions]

    def set_types(self, dict_field_types=None, inplace=False, **kwargs):
        for field_name, field_type in list((dict_field_types or {}).items()) + list(kwargs.items()):
            field_description = self.get_field_description(field_name)
            assert isinstance(field_description, FieldInterface)
            field_description.set_type(get_canonic_type(field_type), inplace=True)
        if not inplace:
            return self

    def get_field_position(self, field: FieldID, skip_errors: bool = False) -> Optional[FieldNo]:
        if isinstance(field, FieldNo):
            if field < self.get_fields_count():
                return field
        elif isinstance(field, FieldName):
            try:
                return self.get_columns().index(field)
            except ValueError:
                pass
            except IndexError:
                pass
            if not skip_errors:
                raise IndexError('There is no field {} in schema: {}'.format(field, self.get_schema_str()))

    def get_fields_positions(self, names: Array):
        columns = self.get_columns()
        return [columns.index(f) for f in names]

    def get_converters(self, src='str', dst='py'):
        converters = list()
        for desc in self.fields_descriptions:
            converters.append(desc.get_converter(src, dst))
        return tuple(converters)

    def get_field_description(self, field_name: FieldID) -> FieldInterface:
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
        return SchemaDescription(self.fields_descriptions)

    def simple_select_fields(self, fields: Array):
        return SchemaDescription(
            [self.get_field_description(f) for f in fields]
        )

    def __iter__(self):
        yield from self.get_fields_descriptions()

    def __add__(self, other: Union[FieldInterface, SchemaInterface, str]) -> SchemaInterface:
        if isinstance(other, (str, int, FieldInterface)):
            return self.append_field(other, inplace=False)
        elif isinstance(other, (SchemaInterface, Iterable)):
            return self.append(other, inplace=False).set_name(None, inplace=False)
        else:
            raise TypeError('Expected other as field or schema, got {} as {}'.format(other, type(other)))
