from typing import Optional, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructInterface, StructRowInterface, FieldInterface, SelectionLoggerInterface, ExtLogger,
        FieldType, DialectType,
        AUTO, Auto, Name, Array, ARRAY_TYPES,
    )
    from base.functions.arguments import update, get_generated_name, get_name, get_names
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.describe_mixin import DescribeMixin
    from functions.secondary import array_functions as fs
    from utils.external import pd, get_use_objects_for_output, DataFrame
    from content.fields.advanced_field import AdvancedField
    from content.selection.abstract_expression import AbstractDescription
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructInterface, StructRowInterface, FieldInterface, SelectionLoggerInterface, ExtLogger,
        FieldType, DialectType,
        AUTO, Auto, Name, Array, ARRAY_TYPES,
    )
    from ...base.functions.arguments import update, get_generated_name, get_name, get_names
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.mixin.describe_mixin import DescribeMixin
    from ...functions.secondary import array_functions as fs
    from ...utils.external import pd, get_use_objects_for_output, DataFrame
    from ..fields.advanced_field import AdvancedField
    from ..selection.abstract_expression import AbstractDescription

Native = StructInterface
Group = Union[Native, Iterable]
StructName = Optional[Name]
Field = Union[Name, dict, FieldInterface]
Type = Union[FieldType, type, Auto]
Comment = Union[StructName, Auto]

META_MEMBER_MAPPING = dict(_data='fields')
GROUP_NO_STR = '===='
GROUP_TYPE_STR = 'GROUP'
DICT_VALID_SIGN = {'True': '-', 'False': 'x', 'None': '-', AUTO.get_value(): '~'}


class FlatStruct(SimpleDataWrapper, DescribeMixin, StructInterface):
    def __init__(
            self,
            fields: Iterable,
            name: StructName = None,
            caption: Optional[str] = None,
            default_type: Type = AUTO,
            exclude_duplicates: bool = False,
            reassign_struct_name: bool = False,
    ):
        name = Auto.acquire(name, get_generated_name(prefix='FieldGroup'))
        self._caption = caption or ''
        super().__init__(name=name, data=list())
        for field_or_group in fields:
            kwargs = dict(
                default_type=default_type, exclude_duplicates=exclude_duplicates,
                reassign_struct_name=reassign_struct_name, inplace=True,
            )
            if isinstance(field_or_group, StructInterface):  # FieldGroup
                self.add_fields(field_or_group.get_fields_descriptions(), **kwargs)
            elif isinstance(field_or_group, list):  # not tuple (tuple can be old-style FieldDescription
                self.add_fields(*field_or_group, **kwargs)
            elif field_or_group:
                self.append_field(field_or_group, **kwargs)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return META_MEMBER_MAPPING

    def get_caption(self) -> str:
        return self._caption

    def set_caption(self, caption: str, inplace: bool) -> Native:
        if inplace:
            self._caption = caption
            return self
        else:
            struct = self.make_new(caption=caption)
            return self._assume_native(struct)

    def caption(self, caption: str) -> Native:
        self._caption = caption
        return self

    def is_defined(self) -> bool:
        return bool(self.get_fields())

    def set_fields(self, fields: Iterable, inplace: bool) -> Optional[Native]:
        struct = self.set_data(data=fields, inplace=inplace, reset_dynamic_meta=False)
        return self._assume_native(struct)

    def get_fields(self) -> list:
        return self.get_data()

    def get_field_names(self) -> list:
        return get_names(self.get_fields())

    def fields(self, fields: Iterable) -> Native:
        self._data = list(fields)
        return self

    def set_field_no(self, no: int, field: Field, inplace: bool) -> Native:
        if inplace:
            self.get_data()[no] = field
            return self
        else:
            struct = self.copy()
            assert isinstance(struct, FlatStruct)
            struct.set_field_no(no=no, field=field, inplace=True)
            return struct

    @staticmethod
    def _is_field(field):
        return hasattr(field, 'get_name') and hasattr(field, 'get_type')

    def append_field(
            self,
            field: Field,
            default_type: FieldType = FieldType.Any,
            before: bool = False,
            exclude_duplicates: bool = True,
            reassign_struct_name: bool = False,
            skip_missing: bool = False,
            inplace: bool = True,
    ) -> Optional[Native]:
        if self._is_field(field):
            field_desc = field
        elif isinstance(field, str):
            field_desc = AdvancedField(field, default_type)
        elif isinstance(field, ARRAY_TYPES):
            field_desc = AdvancedField(*field)
        elif isinstance(field, dict):
            field_desc = AdvancedField(**field)
        elif skip_missing and field is None:
            pass
        else:
            raise TypeError('Expected field, str or dict, got {} as {}'.format(field, type(field)))
        if exclude_duplicates and field_desc.get_name() in self.get_field_names():
            return self
        else:
            if isinstance(field_desc, AdvancedField):
                if reassign_struct_name or not Auto.is_defined(field_desc.get_group_name()):
                    field_desc.set_group_name(self.get_name(), inplace=True)
                    field_desc.set_group_caption(self.get_caption(), inplace=True)
            if before:
                fields = [field_desc] + self.get_fields()
            else:
                fields = self.get_fields() + [field_desc]
            return self.set_fields(fields, inplace=inplace)

    def append(
            self,
            field_or_struct: Union[Field, Group],
            default_type: Optional[Type] = None,
            exclude_duplicates: bool = False,
            reassign_struct_name: bool = False,
            inplace: bool = False,
    ) -> Optional[Native]:
        kwargs = dict(
            default_type=default_type, exclude_duplicates=exclude_duplicates,
            reassign_struct_name=reassign_struct_name, inplace=inplace,
        )
        if isinstance(field_or_struct, StructInterface):
            return self.add_fields(*field_or_struct.get_fields_descriptions(), **kwargs)
        elif isinstance(field_or_struct, Iterable) and not isinstance(field_or_struct, str):
            return self.add_fields(*field_or_struct, **kwargs)
        else:  # isinstance(field_or_struct, Field):
            return self.append_field(field_or_struct, **kwargs)

    def add_fields(
            self,
            *fields,
            default_type: Optional[Type] = None,
            exclude_duplicates: bool = False,
            name: StructName = None,
            reassign_struct_name: bool = False,
            inplace: bool = False,
    ) -> Optional[Native]:
        fields = update(fields)
        if inplace:
            for f in fields:
                self.append(
                    f, default_type=default_type,
                    exclude_duplicates=exclude_duplicates,
                    reassign_struct_name=reassign_struct_name,
                    inplace=True,
                )
        else:
            return self.make_new(fields=self.get_fields_descriptions() + list(fields), name=name)

    def remove_fields(self, *fields, multiple: bool = False, inplace: bool = True):
        removing_fields = update(fields)
        removing_field_names = get_names(removing_fields)
        existing_fields = self.get_fields()
        if inplace:
            for e in existing_fields:
                if get_name(e) in removing_field_names:
                    existing_fields.remove(e)
                    if not multiple:
                        break
        else:
            new_fields = [f for f in existing_fields if get_name(f) not in removing_field_names]
            return self.make_new(new_fields)

    def is_empty(self) -> bool:
        return not self.get_column_count()

    def get_column_count(self) -> int:
        return self.get_fields_count()

    def get_fields_count(self) -> int:
        return len(self.get_fields())

    def get_type_count(self, field_type: Type = AUTO, by_prefix: bool = True) -> int:
        count = 0
        field_type_name = get_name(field_type, or_callable=False)
        for f in self.get_fields():
            if by_prefix:
                is_selected_type = f.get_type_name().startswith(field_type_name)
            else:
                is_selected_type = f.get_type_name() == field_type_name
            if is_selected_type:
                count += 1
        return count

    def get_str_fields_count(self, types: Array = (str, int, float, bool)) -> str:
        total_count = self.get_fields_count()
        type_names = list()
        types_count = list()
        for t in types:
            types_count.append(self.get_type_count(t))
            type_names.append(get_name(t, or_callable=False))
        other_count = total_count - sum(types_count)
        str_fields_count = ' + '.join(['{} {}'.format(c, t) for c, t in zip(types_count, type_names)])
        return '{} total = {} + {} other'.format(total_count, str_fields_count, other_count)

    def get_struct_str(self, dialect: DialectType = DialectType.Python) -> str:
        if not isinstance(dialect, DialectType):
            dialect = DialectType.detect(dialect)
        template = '{}: {}' if dialect in ('str', 'py') else '{} {}'
        field_strings = [template.format(c.get_name(), c.get_type_in(dialect)) for c in self.get_fields()]
        return ', '.join(field_strings)

    def get_columns(self) -> list:
        return [c.get_name() for c in self.get_fields()]

    def get_types_list(self, dialect: Union[DialectType, Auto] = DialectType.String) -> list:
        if Auto.is_defined(dialect):
            return [f.get_type_in(dialect) for f in self.get_fields()]
        else:
            return [f.get_type() for f in self.get_fields()]

    def get_types_dict(self, dialect: Union[DialectType, Auto] = AUTO) -> dict:
        names = map(lambda f: get_name(f), self.get_fields())
        types = self.get_types_list(dialect)
        return dict(zip(names, types))

    def get_types(self, dialect: DialectType = DialectType.String, as_list: bool = True) -> Union[list, dict]:
        if as_list:
            return self.get_types_list(dialect)
        else:
            return self.get_types_dict(dialect)

    def set_types(
            self,
            dict_field_types: Optional[dict] = None,
            inplace: bool = True,
            **kwargs
    ) -> Native:
        if inplace:
            return self.types(dict_field_types=dict_field_types, **kwargs) or self
        else:
            struct = self.copy()
            assert isinstance(struct, FlatStruct)
            return struct.types(dict_field_types=dict_field_types, **kwargs)

    def types(self, dict_field_types: Optional[dict] = None, **kwargs) -> Native:
        for field_name, field_type in list((dict_field_types or {}).items()) + list(kwargs.items()):
            field = self.get_field_description(field_name)
            assert hasattr(field, 'set_type'), 'Expected SimpleField or FieldDescription, got {}'.format(field)
            field.set_type(FieldType.detect_by_type(field_type), inplace=True)
        return self

    def common_type(self, field_type: Union[FieldType, type]) -> Native:
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

    def get_converters(self, src: DialectType = DialectType.String, dst: DialectType = DialectType.Python) -> tuple:
        converters = list()
        for desc in self.get_fields():
            converters.append(desc.get_converter(src, dst))
        return tuple(converters)

    def get_fields_descriptions(self) -> list:
        return self.get_fields()

    def get_invalid_columns(self) -> Iterable:
        for f in self.get_fields():
            if hasattr(f, 'is_valid'):
                if not f.is_valid():
                    yield f

    def get_invalid_fields_count(self) -> Optional[int]:
        count = 0
        for _ in self.get_invalid_columns():
            count += 1
        return count

    def is_valid_struct(self) -> bool:
        for _ in self.get_invalid_columns():
            return False
        return True

    def is_valid_row(self, row: Union[Iterable, StructRowInterface]) -> bool:
        for value, field_type in zip(row, self.get_types_list(DialectType.Python)):
            if not isinstance(value, field_type):
                return False
        return True

    def get_validation_errors(self, row: Union[Iterable, StructInterface]) -> list:
        if isinstance(row, StructRowInterface) or hasattr(row, 'get_data'):
            row = row.get_data()
        validation_errors = list()
        for value, field_description in zip(row, self.get_fields_descriptions()):
            assert isinstance(field_description, FieldInterface)
            field_type = field_description.get_type()
            if not field_type.isinstance(value):
                template = '(FlatStruct) Field {}: type {} expected, got {} (value={})'
                msg = template.format(field_description.get_name(), field_type, type(value), value)
                validation_errors.append(msg)
        return validation_errors

    @staticmethod
    def convert_to_native(other: StructInterface) -> Native:
        if isinstance(other, FlatStruct) or hasattr(other, 'get_caption'):
            return other
        elif isinstance(other, StructInterface):
            return FlatStruct(other, name='FlatStruct', caption='(unknown struct type)')
        else:
            return FlatStruct(other, name='UNKNOWN', caption='(unknown struct type)')

    def get_struct_comparison_dict(self, other: StructInterface) -> dict:
        alias = dict(a_field='added', b_field='removed', ab_field='saved', as_dict=True)
        return fs.compare_lists(**alias)(other, self)

    def get_struct_comparison_iter(self, other: StructInterface, message: Optional[str] = None) -> Iterable:
        if Auto.is_defined(message):
            title = '{} {}'.format(self.__repr__(), message)
        else:
            title = self.__repr__()
        comparison = self.get_struct_comparison_dict(other)
        counts = {k: len(v) for k, v in comparison.items()}
        added_names = get_names(comparison.get('added'))
        removed_names = get_names(comparison.get('removed'))
        if added_names or removed_names:
            message = '{}: {saved} fields will be saved, {added} added, {removed} removed'.format(title, **counts)
            yield message
            if added_names:
                yield 'Added {} fields: {}'.format(len(added_names), ', '.join(added_names))
            if removed_names:
                yield 'Removed {} fields: {}'.format(len(removed_names), ', '.join(removed_names))
        else:
            yield '{}: Struct is actual, will not be changed'.format(title)

    def validate_about(self, standard: StructInterface, ignore_moved: bool = False) -> Native:
        expected_struct = self.convert_to_native(standard)
        remaining_struct = expected_struct.copy()
        assert isinstance(expected_struct, FlatStruct), 'got {}'.format(expected_struct)
        assert isinstance(remaining_struct, FlatStruct), 'got {}'.format(remaining_struct)
        updated_struct = FlatStruct([])
        for pos_received, f_received in enumerate(self.get_fields()):
            assert isinstance(f_received, AdvancedField)
            f_name = f_received.get_name()
            if f_name in updated_struct.get_field_names():
                is_valid = False
                warning = 'DUPLICATE_IN_DATA' if f_name in remaining_struct.get_field_names() else 'DUPLICATE'
                f_expected = updated_struct.get_field_description(f_name)
                f_updated = f_expected.set_valid(is_valid, inplace=False)
            elif f_name in expected_struct.get_field_names():
                is_valid = True
                pos_expected = expected_struct.get_field_position(f_name)
                warning = None if pos_received == pos_expected or ignore_moved else 'MOVED'
                f_expected = expected_struct.get_field_description(f_name)
                f_updated = f_expected.set_valid(is_valid, inplace=False)
            else:
                is_valid = False
                warning = 'UNEXPECTED'
                message = 'field has been found in actual struct, but not in expected standard struct'
                caption = '{} ({})'.format(f_received.get_caption(), message)
                f_updated = f_received.set_valid(is_valid, inplace=False).set_caption(caption, inplace=False)
            if warning:
                caption = '[{}] {}'.format(warning, f_updated.get_caption() or '')
                f_updated = f_updated.set_caption(caption, inplace=False)
            updated_struct.append_field(f_updated, exclude_duplicates=ignore_moved)
            if f_name in remaining_struct.get_field_names():
                remaining_struct.remove_fields(f_name, inplace=True)

        for f_remaining in remaining_struct.get_columns():
            f_name = get_name(f_remaining)
            is_valid = False
            f_expected = expected_struct.get_field_description(f_name)
            if f_name in updated_struct.get_field_names():
                warning = 'DUPLICATE_IN_STRUCT'
            else:
                warning = 'MISSING_IN_FILE'
            caption = '[{}] {}'.format(warning, f_expected.get_caption() or '')
            f_updated = f_expected.set_valid(is_valid, inplace=False).set_caption(caption, inplace=False)
            updated_struct.append_field(f_updated, exclude_duplicates=ignore_moved)
        self.set_fields(updated_struct.get_fields(), inplace=True)
        return self

    def get_validation_message(self, standard: Union[StructInterface, Auto, None] = AUTO) -> str:
        if Auto.is_defined(standard):
            self.validate_about(standard)
        if self.is_valid_struct():
            message = 'struct has {} valid columns:'.format(self.get_column_count())
        else:
            valid_count = self.get_column_count() - self.get_invalid_fields_count()
            message = '[INVALID] struct has {} columns = {} valid + {} invalid:'.format(
                self.get_column_count(), valid_count, self.get_invalid_fields_count(),
            )
        return message

    @staticmethod
    def get_struct_detected_by_title_row(title_row: Iterable) -> Native:
        struct = FlatStruct([])
        for name in title_row:
            field_type = FieldType.detect_by_name(name)
            struct.append_field(AdvancedField(name, field_type))
        return struct

    def copy(self) -> Native:
        return FlatStruct(fields=list(self.get_fields()), name=self.get_name())

    def simple_select_fields(self, fields: Iterable) -> Group:
        return FlatStruct(
            [self.get_field_description(f) for f in fields]
        )

    def get_fields_tuples(self) -> Iterable[tuple]:  # (name, type, caption)
        for f in self.get_fields():
            if isinstance(f, AdvancedField):
                field_name = f.get_name()
                field_type_name = f.get_type_name()
                field_caption = f.get_caption() or ''
                field_is_valid = str(f.is_valid())
                group_name = f.get_group_name()
                group_caption = f.get_group_caption()
            elif isinstance(f, tuple) and len(f) == 2:  # old-style FieldDescription
                field_name = f[0]
                field_type_name = FieldType(f[1]).get_name()
                field_is_valid = '?'
                field_caption, group_name, group_caption = '', '', ''
            else:
                field_name = str(f)
                field_type_name = FieldType.get_default()
                field_caption, field_is_valid, group_name, group_caption = '', '', '', ''
            str_field_is_valid = DICT_VALID_SIGN.get(field_is_valid, field_is_valid[0])
            yield field_name, field_type_name, field_caption, str_field_is_valid, group_name, group_caption

    def get_field_description(self, field_name: Name, skip_missing: bool = False) -> Union[Field, AdvancedField]:
        field_position = self.get_field_position(field_name)
        if field_position is not None:
            return self.get_fields()[field_position]
        elif not skip_missing:
            message = 'Field {} not found (existing fields: {})'
            raise IndexError(message.format(field_name, ', '.join(self.get_field_names())))

    def get_struct_description(self, include_header: bool = False) -> Iterable[tuple]:
        group_name = self.get_name()
        group_caption = self.get_caption()
        if include_header:
            yield GROUP_NO_STR, GROUP_TYPE_STR, group_name or '', group_caption, ''
        prev_group_name = group_name
        for n, field_tuple in enumerate(self.get_fields_tuples()):
            f_name, f_type_name, f_caption, f_valid, group_name, group_caption = field_tuple
            is_next_group = group_name != prev_group_name
            if is_next_group:
                yield GROUP_NO_STR, GROUP_TYPE_STR, group_name, group_caption, ''
            yield n, f_type_name, f_name or '', f_caption, f_valid
            prev_group_name = group_name

    def get_group_header(self, name: Comment = AUTO, caption: Comment = AUTO, comment: Comment = None) -> Iterable[str]:
        is_title_row = name == AUTO
        name = Auto.acquire(name, self.get_name())
        caption = Auto.acquire(caption, self.get_caption())
        if Auto.is_defined(name):
            yield name
        if Auto.is_defined(caption):
            yield caption
        if is_title_row:
            yield self.get_str_fields_count()
        if Auto.is_defined(comment):
            yield comment

    @staticmethod
    def _get_describe_template(example) -> tuple:
        if example:
            columns = ('V', 'N', 'TYPE', 'NAME', 'EXAMPLE', 'CAPTION')
            template = ' {:<1}  {:<3} {:<8} {:<28} {:<14} {:<56}'
        else:
            columns = ('V', 'N', 'TYPE', 'NAME', 'CAPTION')
            template = ' {:<1}  {:<3} {:<8} {:<28} {:<72}'
        return columns, template

    def get_struct_repr_lines(
            self,
            example: Optional[dict] = None,
            separate_by_tabs: bool = False,
            select_fields: Optional[Array] = None,
            count: Optional[int] = None
    ) -> Generator:
        columns, template = self._get_describe_template(example)
        yield '\t'.join(columns) if separate_by_tabs else template.format(*columns)
        for (n, type_name, name, caption, is_valid) in self.get_struct_description(include_header=False):
            if type_name == GROUP_TYPE_STR:
                yield ''
                for line in self.get_group_header(name, caption=caption):
                    yield line
            else:
                if name in (select_fields or []):
                    is_valid = '>' if is_valid == '.' else str(is_valid).upper()
                if example:
                    value = str(example.get(name))
                    row = (is_valid, n, type_name, name, value, caption)
                else:
                    row = (is_valid, n, type_name, name, caption)
                yield '\t'.join(row) if separate_by_tabs else template.format(*row)
            if Auto.is_defined(count):
                if n >= count - 1:
                    break

    def describe(
            self,
            example: Optional[dict] = None,
            count: Optional[int] = None,
            as_dataframe: bool = False,
            separate_by_tabs: bool = False,
            show_header: bool = True,
            comment: Comment = None,
            select_fields: Optional[Array] = None,
            logger: Union[ExtLogger, Auto] = AUTO,
    ) -> Optional[DataFrame]:
        log = logger.log if Auto.is_defined(logger) else print
        if show_header:
            for line in self.get_group_header(comment=comment):
                log(line)
            log('')
        if as_dataframe:
            return self.show()
        else:
            struct_description_lines = self.get_struct_repr_lines(
                example=example, separate_by_tabs=separate_by_tabs, select_fields=select_fields,
                count=count,
            )
            for line in struct_description_lines:
                log(line)

    def get_dataframe(self) -> DataFrame:
        data = self.get_struct_description(include_header=True)
        columns = ('n', 'type', 'name', 'caption', 'valid')
        return DataFrame(data, columns=columns)

    def show(self, as_dataframe: Union[bool, Auto] = AUTO) -> Optional[DataFrame]:
        as_dataframe = Auto.acquire(as_dataframe, get_use_objects_for_output())
        if as_dataframe:
            return self.get_dataframe()
        else:
            return self.describe(as_dataframe=False)

    @staticmethod
    def _assume_native(struct) -> Native:
        return struct

    def __repr__(self):
        return self.get_struct_str(None)

    def __str__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.get_struct_str('str'))

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

    def __add__(self, other: Union[FieldInterface, StructInterface, Name]) -> Native:
        if isinstance(other, (str, int, FieldInterface)):
            return self.append_field(other, inplace=False)
        elif isinstance(other, (StructInterface, Iterable)):
            return self.append(other, inplace=False).set_name(None, inplace=False)
        else:
            raise TypeError('Expected other as field or struct, got {} as {}'.format(other, type(other)))


AdvancedField.set_struct_builder(FlatStruct)
