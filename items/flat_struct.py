from typing import Optional, Union, Iterable, NoReturn

try:  # Assume we're a sub-module in a package.
    from interfaces import (
        SchemaInterface, StructRowInterface, FieldInterface, SelectionLoggerInterface,
        FieldType,
        AUTO, Auto, Name, Array, ARRAY_TYPES,
    )
    from utils import arguments as arg
    from utils.external import pd, get_use_objects_for_output, DataFrame
    from base.abstract.simple_data import SimpleDataWrapper
    from fields.advanced_field import AdvancedField
    from selection.abstract_expression import AbstractDescription
    from connectors.databases import dialect as di
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import (
        SchemaInterface, StructRowInterface, FieldInterface, SelectionLoggerInterface,
        FieldType,
        AUTO, Auto, Name, Array, ARRAY_TYPES,
    )
    from ..utils import arguments as arg
    from ..utils.external import pd, get_use_objects_for_output, DataFrame
    from ..base.abstract.simple_data import SimpleDataWrapper
    from ..fields.advanced_field import AdvancedField
    from ..selection.abstract_expression import AbstractDescription
    from ..connectors.databases import dialect as di

Native = SchemaInterface
Group = Union[Native, Iterable]
StructName = Optional[Name]
Dialect = Optional[Name]
Field = Union[Name, dict, FieldInterface]
Type = Union[FieldType, type, Auto]
Comment = Union[StructName, Auto]

META_MEMBER_MAPPING = dict(_data='fields')
GROUP_NO_STR = '===='
GROUP_TYPE_STR = 'GROUP'
DICT_VALID_SIGN = {'True': '-', 'False': 'x', 'None': '-', arg.DEFAULT.get_value(): '~'}


class FlatStruct(SimpleDataWrapper, SchemaInterface):
    def __init__(
            self,
            fields: Iterable,
            name: StructName = None,
            caption: Optional[str] = None,
            default_type: Type = arg.DEFAULT,
    ):
        name = arg.undefault(name, arg.get_generated_name(prefix='FieldGroup'))
        self._caption = caption or ''
        super().__init__(name=name, data=list())
        for field_or_group in fields:
            if isinstance(field_or_group, SchemaInterface):  # FieldGroup
                self.add_fields(field_or_group.get_fields_descriptions(), default_type=default_type, inplace=True)
            elif isinstance(field_or_group, list):  # not tuple (tuple can be old-style FieldDescription
                self.add_fields(*field_or_group, default_type=default_type, inplace=True)
            elif field_or_group:
                self.append_field(field_or_group, default_type=default_type, inplace=True)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return META_MEMBER_MAPPING

    def get_caption(self) -> str:
        return self._caption

    def set_caption(self, caption: str, inplace: bool) -> Optional[Native]:
        if inplace:
            self._caption = caption
        else:
            return self.make_new(caption=caption)

    def caption(self, caption: str) -> Native:
        self._caption = caption
        return self

    def set_fields(self, fields: Iterable, inplace: bool) -> Optional[Native]:
        return self.set_data(data=fields, inplace=inplace, reset_dynamic_meta=False)

    def get_fields(self) -> list:
        return list(self.get_data())

    def get_field_names(self) -> list:
        return [f.get_name() for f in self.get_fields()]

    def fields(self, fields: Iterable) -> Native:
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
            add_group_props: bool = True,
    ) -> Optional[SchemaInterface]:
        if self._is_field(field):
            field_desc = field
        elif isinstance(field, str):
            field_desc = AdvancedField(field, default_type)
        elif isinstance(field, ARRAY_TYPES):
            field_desc = AdvancedField(*field)
        elif isinstance(field, dict):
            field_desc = AdvancedField(**field)
        else:
            raise TypeError('Expected field, str or dict, got {} as {}'.format(field, type(field)))
        if add_group_props and hasattr(field_desc, 'get_group_name'):  # isinstance(field_desc, AdvancedField)
            if not arg.is_defined(field_desc.get_group_name()):
                field_desc.set_group_name(self.get_name(), inplace=True)
            if not arg.is_defined(field_desc.get_group_caption()):
                field_desc.set_group_caption(self.get_caption(), inplace=True)
        if before:
            fields = [field_desc] + self.get_fields()
        else:
            fields = self.get_fields() + [field_desc]
        return self.set_fields(fields, inplace=inplace)

    def append(
            self,
            field_or_group: Union[Field, Group],
            default_type: Optional[Type] = None,
            inplace: bool = False,
    ) -> Optional[SchemaInterface]:
        if isinstance(field_or_group, SchemaInterface):
            return self.add_fields(*field_or_group.get_fields_descriptions(), default_type, inplace=inplace)
        elif isinstance(field_or_group, Iterable) and not isinstance(field_or_group, str):
            return self.add_fields(*field_or_group, default_type=default_type, inplace=inplace)
        else:  # isinstance(field_or_group, Field):
            return self.append_field(field_or_group, default_type=default_type, inplace=inplace, add_group_props=False)

    def add_fields(
            self,
            *fields,
            default_type: Optional[Type] = None,
            inplace: bool = False,
            name: StructName = None,
    ) -> Optional[SchemaInterface]:
        fields = arg.update(fields)
        if inplace:
            for f in fields:
                self.append(f, default_type=default_type, inplace=True)
        else:
            return FlatStruct(self.get_fields_descriptions() + list(fields), name=name)

    def remove_fields(self, *fields, inplace: bool = True, name: StructName = None):
        removing_fields = arg.update(fields)
        removing_field_names = arg.get_names(removing_fields)
        existing_fields = self.get_data()
        if inplace:
            for e in existing_fields:
                if arg.get_name(e) in removing_field_names:
                    existing_fields.remove(e)
        else:
            new_fields = [f for f in existing_fields if arg.get_name(f) not in removing_field_names]
            return FlatStruct(new_fields, name=name)

    def get_fields_count(self) -> int:
        return len(self.get_fields())

    def get_type_count(self, field_type: Type = arg.DEFAULT, by_prefix: bool = True) -> int:
        count = 0
        field_type = FieldType(field_type)
        for f in self.get_fields():
            if by_prefix:
                is_selected_type = f.get_type_name().startswith(field_type.get_name())
            else:
                is_selected_type = f.get_type_name() == field_type.get_name()
            if is_selected_type:
                count += 1
        return count

    def get_str_fields_count(self, types: Array = (str, int, float, bool)) -> str:
        total_count = self.get_fields_count()
        types_count = list()
        for t in types:
            types_count.append(self.get_type_count(t))
        other_count = total_count - sum(types_count)
        str_fields_count = ' + '.join(['{} {}'.format(c, t) for c, t in zip(types, types_count)])
        return '{} total = {} + {} other'.format(total_count, str_fields_count, other_count)

    def get_schema_str(self, dialect: Dialect = 'py') -> str:
        if dialect is not None and dialect not in di.DIALECTS:
            dialect = di.get_dialect_for_connector(dialect)
        template = '{}: {}' if dialect in ('str', 'py') else '{} {}'
        field_strings = [template.format(c.get_name(), c.get_type_in(dialect)) for c in self.get_fields()]
        return ', '.join(field_strings)

    def get_columns(self) -> list:
        return [c.get_name() for c in self.get_fields()]

    def get_types(self, dialect: Dialect) -> list:
        return [c.get_type_in(dialect) for c in self.get_fields()]

    def set_types(
            self,
            dict_field_types: Optional[dict] = None,
            inplace: bool = True,
            **kwargs
    ) -> Optional[SchemaInterface]:
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

    def get_field_position(self, field: Name) -> Optional[int]:
        if isinstance(field, int):
            if field < self.get_fields_count():
                return field
        elif isinstance(field, str):
            try:
                return self.get_columns().index(field)
            except ValueError or IndexError:
                return None
        elif isinstance(field, FieldInterface):
            return self.get_field_position(field.get_name())

    def get_fields_positions(self, names: Iterable) -> list:
        columns = self.get_columns()
        return [columns.index(f) for f in names]

    def get_converters(self, src: Dialect = 'str', dst: Dialect = 'py') -> tuple:
        converters = list()
        for desc in self.get_fields():
            converters.append(desc.get_converter(src, dst))
        return tuple(converters)

    def get_fields_descriptions(self) -> list:
        return self.get_fields()

    def is_valid_row(self, row: Union[Iterable, StructRowInterface]) -> bool:
        for value, field_type in zip(row, self.get_types('py')):
            if not isinstance(value, field_type):
                return False
        return True

    def copy(self):
        return FlatStruct(name=self.get_name(), fields=self.get_fields())

    def simple_select_fields(self, fields: Iterable) -> Group:
        return FlatStruct(
            [self.get_field_description(f) for f in fields]
        )

    def get_fields_tuples(self) -> Iterable[tuple]:  # name, type, caption
        for f in self.get_fields():
            if isinstance(f, AdvancedField):
                field_name = f.get_name()
                field_type_name = f.get_type_name()
                field_caption = f.get_caption() or ''
                group_name = f.get_group_name()
                group_caption = f.get_group_caption()
            elif isinstance(f, tuple) and len(f) == 2:  # old-style FieldDescription
                field_name = f[0]
                field_type_name = FieldType(f[1]).get_name()
                field_caption, group_name, group_caption = '', '', ''
            else:
                field_name = str(f)
                field_type_name = FieldType.get_default()
                field_caption, group_name, group_caption = '', '', ''
            yield field_name, field_type_name, field_caption, group_name, group_caption

    def get_field_description(self, field_name: Name) -> Union[FieldInterface, AdvancedField]:
        field_position = self.get_field_position(field_name)
        assert field_position is not None, 'Field {} not found (existing fields: {})'.format(
            field_name, self.get_columns(),
        )
        return self.get_fields()[field_position]

    def get_struct_description(self, include_header: bool = False) -> Iterable[tuple]:
        group_name = self.get_name()
        group_caption = self.get_caption()
        if include_header:
            yield (GROUP_NO_STR, GROUP_TYPE_STR, group_name, group_caption)
        prev_group_name = group_name
        for n, (f_name, f_type_name, f_caption, group_name, group_caption) in enumerate(self.get_fields_tuples()):
            is_new_group = group_name != prev_group_name
            if is_new_group:
                yield (GROUP_NO_STR, GROUP_TYPE_STR, group_name, group_caption)
            yield (n, f_type_name, f_name, f_caption)

    def get_group_header(self, name=AUTO, caption=AUTO) -> Iterable[str]:
        is_title_row = name == arg.DEFAULT
        name = arg.undefault(name, self.get_name())
        caption = arg.undefault(caption, self.get_caption())
        if name:
            yield name
        if caption:
            yield caption
        if is_title_row:
            yield self.get_str_fields_count()
        yield ''

    @staticmethod
    def _get_describe_template(example) -> tuple:
        if example:
            columns = ('N', 'TYPE', 'NAME', 'EXAMPLE', 'CAPTION')
            template = '    {:<03} {:<8} {:<24} {:<14} {:<64}'
        else:
            columns = ('N', 'TYPE', 'NAME', 'CAPTION')
            template = '    {:<3} {:<8} {:<24} {:<80}'
        return columns, template

    def describe(self, separate_by_tabs: bool = False, example: Optional[dict] = None, logger=arg.DEFAULT) -> NoReturn:
        if arg.is_defined(logger):
            log = logger.log
        else:
            log = print
        for line in self.get_group_header():
            log(line)
        columns, template = self._get_describe_template(example)
        log('\t'.join(columns) if separate_by_tabs else template.format(*columns))
        for (n, type_name, name, caption) in self.get_struct_description(include_header=False):
            if type_name == GROUP_TYPE_STR:
                for line in self.get_group_header(name, caption):
                    log(line)
            else:
                if example:
                    value = example.get(name)
                    row = (n, type_name, name, value, caption)
                else:
                    row = (n, type_name, name, caption)
                log('\t'.join(row) if separate_by_tabs else template.format(*row))

    def get_dataframe(self) -> DataFrame:
        return DataFrame(self.get_struct_description(include_header=True))

    def show(self, as_dataframe: Union[bool, Auto] = AUTO) -> Optional[DataFrame]:
        as_dataframe = arg.undefault(as_dataframe, get_use_objects_for_output())
        if as_dataframe:
            return self.get_dataframe()
        else:
            return self.describe()

    def __repr__(self):
        return self.get_schema_str(None)

    def __str__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.get_schema_str('str'))

    def __iter__(self):
        yield from self.get_fields_descriptions()

    def __getitem__(self, item: Union[Name, slice]):
        if isinstance(item, slice):
            return FlatStruct(self.get_fields_descriptions()[item])
        elif isinstance(item, int):
            return self.get_fields_descriptions()[item]
        else:  # elif isinstance(item, str):
            for f in self.get_fields_descriptions():
                if f.get_name() == item:
                    return f
            raise ValueError('Field with name {} not found (in group {})'.format(item, self))

    def __add__(self, other: Union[FieldInterface, SchemaInterface, Name]) -> SchemaInterface:
        if isinstance(other, (str, int, FieldInterface)):
            return self.append_field(other, inplace=False)
        elif isinstance(other, (SchemaInterface, Iterable)):
            return self.append(other, inplace=False).set_name(None, inplace=False)
        else:
            raise TypeError('Expected other as field or schema, got {} as {}'.format(other, type(other)))
