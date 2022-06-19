from typing import Optional, Iterable, Iterator, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructInterface, StructRowInterface, FieldInterface, RepresentationInterface,
        SelectionLoggerInterface, ExtLogger,
        ValueType, DialectType,
        AUTO, Auto, AutoCount, Name, Array, ARRAY_TYPES, ROW_SUBCLASSES, RECORD_SUBCLASSES,
    )
    from base.functions.arguments import update, get_generated_name, get_name, get_names
    from base.constants.chars import EMPTY, REPR_DELIMITER, TITLE_PREFIX, ITEM, DEL, ABOUT, JUPYTER_LINE_LEN
    from base.abstract.simple_data import SimpleDataWrapper, DEFAULT_ROWS_COUNT
    from base.mixin.iter_data_mixin import IterDataMixin
    from functions.secondary import array_functions as fs
    from utils.external import pd, get_use_objects_for_output, DataFrame
    from content.fields.any_field import AnyField
    from content.items.simple_items import SelectableItem, is_row, is_record
    from content.selection.abstract_expression import AbstractDescription
    from content.selection.selectable_mixin import SelectableMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructInterface, StructRowInterface, FieldInterface, RepresentationInterface,
        SelectionLoggerInterface, ExtLogger,
        ValueType, DialectType,
        AUTO, Auto, AutoCount, Name, Array, ARRAY_TYPES, ROW_SUBCLASSES, RECORD_SUBCLASSES,
    )
    from ...base.functions.arguments import update, get_generated_name, get_name, get_names
    from ...base.constants.chars import EMPTY, REPR_DELIMITER, TITLE_PREFIX, ITEM, DEL, ABOUT, JUPYTER_LINE_LEN
    from ...base.abstract.simple_data import SimpleDataWrapper, DEFAULT_ROWS_COUNT
    from ...base.mixin.iter_data_mixin import IterDataMixin
    from ...functions.secondary import array_functions as fs
    from ...utils.external import pd, get_use_objects_for_output, DataFrame
    from ..fields.any_field import AnyField
    from ..items.simple_items import SelectableItem, is_row, is_record
    from ..selection.abstract_expression import AbstractDescription
    from ..selection.selectable_mixin import SelectableMixin

Native = StructInterface
Group = Union[Native, Iterable]
StructName = Optional[Name]
Field = Union[Name, dict, FieldInterface]
Type = Union[ValueType, type, Auto]
Comment = Union[StructName, Auto]

META_MEMBER_MAPPING = dict(_data='fields')
GROUP_TYPE_STR = 'GROUP'
DICT_VALID_SIGN = {'True': ITEM, 'False': DEL, 'None': ITEM, AUTO.get_value(): ABOUT}

COMPARISON_TAGS = dict(
    this_only='THIS_ONLY', other_only='OTHER_ONLY', moved='MOVED',
    this_duplicated='DUPLICATED_HERE', other_duplicated='DUPLICATED_THERE', both_duplicated='DUPLICATED',
)
VALIDATION_TAGS = dict(
    this_only='UNEXPECTED', other_only='MISSING_IN_DATA', moved='MOVED',
    this_duplicated='DUPLICATE_IN_DATA', other_duplicated='DUPLICATE_IN_STRUCT', both_duplicated='DUPLICATED',
)


class FlatStruct(SimpleDataWrapper, SelectableMixin, IterDataMixin, StructInterface):
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
        self._caption = caption or EMPTY
        super().__init__(name=name, data=list())
        for field_or_struct in fields:
            kwargs = dict(
                default_type=default_type, exclude_duplicates=exclude_duplicates,
                reassign_struct_name=reassign_struct_name, inplace=True,
            )
            if isinstance(field_or_struct, StructInterface) or hasattr(field_or_struct, 'get_fields_descriptions'):
                self.add_fields(field_or_struct.get_fields_descriptions(), **kwargs)
            elif isinstance(field_or_struct, list):  # not tuple (tuple can be old-style FieldDescription
                self.add_fields(*field_or_struct, **kwargs)
            elif field_or_struct:
                self.append_field(field_or_struct, **kwargs)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return META_MEMBER_MAPPING

    def get_caption(self) -> str:
        return self._caption

    def set_caption(self, caption: str, inplace: bool = True) -> Native:
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
    def _is_field(field) -> bool:
        if isinstance(field, (FieldInterface, AnyField)):
            return True
        else:
            return hasattr(field, 'get_name') and hasattr(field, 'get_value_type')

    def append_field(
            self,
            field: Field,
            default_type: ValueType = ValueType.Any,
            before: bool = False,
            exclude_duplicates: bool = True,
            reassign_struct_name: bool = False,
            skip_missing: bool = False,
            inplace: bool = True,
    ) -> Optional[Native]:
        if self._is_field(field):
            field_desc = field
        elif isinstance(field, str):
            field_desc = AnyField(field, value_type=default_type)
        elif isinstance(field, ARRAY_TYPES):
            field_desc = AnyField(*field)
        elif isinstance(field, dict):
            field_desc = AnyField(**field)
        elif skip_missing and field is None:
            return None
        else:
            raise TypeError('Expected field, str or dict, got {} as {}'.format(field, type(field)))
        if exclude_duplicates and field_desc.get_name() in self.get_field_names():
            return self
        else:
            if isinstance(field_desc, (FieldInterface, AnyField)):
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
        elif isinstance(field_or_struct, FieldInterface) or hasattr(field_or_struct, 'get_value_type'):  ###
            return self.append_field(field_or_struct, **kwargs)
        elif isinstance(field_or_struct, Iterable) and not isinstance(field_or_struct, str):
            return self.add_fields(*field_or_struct, **kwargs)
        else:  # isinstance(field_or_struct, str):  # SimpleField
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
            struct = self.make_new(fields=self.get_fields_descriptions() + list(fields), name=name)
            return self._assume_native(struct)

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

    def get_count(self) -> int:
        return self.get_fields_count()

    def get_column_count(self) -> int:
        return self.get_fields_count()

    def get_fields_count(self) -> int:
        return len(self.get_fields())

    def get_type_count(self, field_type: Type = AUTO, by_prefix: bool = True) -> int:
        count = 0
        field_type_name = get_name(field_type, or_callable=False)
        for f in self.get_fields():
            if by_prefix:
                is_selected_type = f.get_value_type_name().startswith(field_type_name)
            else:
                is_selected_type = f.get_value_type_name() == field_type_name
            if is_selected_type:
                count += 1
        return count

    def get_field_representations(self) -> Generator:
        for f in self.get_fields():
            if isinstance(f, (FieldInterface, AnyField)) or hasattr(f, 'get_representation'):
                yield f.get_representation()
            else:
                yield None

    def get_min_str_len(self, delimiter: str = REPR_DELIMITER, default_field_len: int = 0) -> Optional[int]:
        delimiter_len = len(delimiter)
        min_str_len = -delimiter_len
        for r in self.get_field_representations():
            if isinstance(r, RepresentationInterface) or hasattr(r, 'get_min_total_len'):
                field_len = r.get_min_total_len()
            else:
                field_len = default_field_len
            min_str_len += field_len + delimiter_len
        if min_str_len >= 0:
            return min_str_len

    def get_max_str_len(self, delimiter: str = REPR_DELIMITER, default_field_len: int = 0) -> Optional[int]:
        delimiter_len = len(delimiter)
        max_str_len = -delimiter_len
        for r in self.get_field_representations():
            if isinstance(r, RepresentationInterface) or hasattr(r, 'get_max_total_len'):
                field_len = r.get_max_total_len()
            else:
                field_len = default_field_len
            max_str_len += field_len + delimiter_len
        if max_str_len >= 0:
            return max_str_len

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
            return [f.get_value_type() for f in self.get_fields()]

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
            assert hasattr(field, 'set_value_type'), 'Expected SimpleField or FieldDescription, got {}'.format(field)
            field.set_value_type(ValueType.detect_by_type(field_type), inplace=True)
        return self

    def common_type(self, field_type: Union[ValueType, type]) -> Native:
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
            field_type = field_description.get_value_type()
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

    def compare_with(self, other: StructInterface, ignore_moved: bool = True, tags: dict = AUTO, set_valid: bool = False) -> Native:
        tags = Auto.acquire(tags, COMPARISON_TAGS)
        expected_struct = self.convert_to_native(other)
        remaining_struct = expected_struct.copy()
        assert isinstance(expected_struct, StructInterface) or hasattr(expected_struct, 'get_field_names'), 'got {}'.format(expected_struct)
        assert isinstance(remaining_struct, StructInterface) or hasattr(remaining_struct, 'get_field_names'), 'got {}'.format(remaining_struct)
        updated_struct = FlatStruct([])
        for pos_received, f_received in enumerate(self.get_fields()):
            assert isinstance(f_received, (FieldInterface, AnyField))
            f_name = f_received.get_name()
            if f_name in updated_struct.get_field_names():
                is_valid = False
                tag = tags['this_duplicated'] if f_name in remaining_struct.get_field_names() else tags['both_duplicated']
                f_expected = updated_struct.get_field_description(f_name)
                f_updated = f_expected.set_valid(is_valid, inplace=False) if set_valid else f_expected
            elif f_name in expected_struct.get_field_names():
                is_valid = True
                pos_expected = expected_struct.get_field_position(f_name)
                tag = None if pos_received == pos_expected or ignore_moved else tags['moved']
                f_expected = expected_struct.get_field_description(f_name)
                f_updated = f_expected.set_valid(is_valid, inplace=False) if set_valid else f_expected
            else:
                is_valid = False
                tag = tags['this_only']
                f_updated = f_received.set_valid(is_valid, inplace=False) if set_valid else f_received
            if tag:
                caption = '[{}] {}'.format(tag, f_updated.get_caption() or EMPTY)
                f_updated = f_updated.set_caption(caption, inplace=False)
            updated_struct.append_field(f_updated, exclude_duplicates=ignore_moved)
            if f_name in remaining_struct.get_field_names():
                remaining_struct.remove_fields(f_name, inplace=True)

        for f_remaining in remaining_struct.get_columns():
            f_name = get_name(f_remaining)
            is_valid = False
            f_expected = expected_struct.get_field_description(f_name)
            caption = f_expected.get_caption() or EMPTY
            if f_name in updated_struct.get_field_names():
                tag = tags['other_duplicated']
            else:
                tag = tags['other_only']
            if tag not in caption:
                caption = '[{}] {}'.format(tag, caption or EMPTY)
            f_updated = f_expected.set_valid(is_valid, inplace=False) if set_valid else f_expected
            f_updated = f_updated.set_caption(caption, inplace=False)
            updated_struct.append_field(f_updated, exclude_duplicates=ignore_moved)
        self.set_fields(updated_struct.get_fields(), inplace=True)
        return self

    def validate_about(self, standard: StructInterface, ignore_moved: bool = False) -> Native:
        tags = VALIDATION_TAGS
        return self.compare_with(standard, ignore_moved=ignore_moved, tags=tags, set_valid=True)

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
            field_type = ValueType.detect_by_name(name)
            struct.append_field(AnyField(name, value_type=field_type))
        return struct

    def copy(self) -> Native:
        return FlatStruct(fields=list(self.get_fields()), name=self.get_name())

    def format(self, *args, delimiter: str = REPR_DELIMITER, skip_errors: bool = False) -> str:
        if len(args) == 1 and isinstance(args[0], (*ROW_SUBCLASSES, *RECORD_SUBCLASSES)):
            item = args[0]
        else:
            item = args
        formatted_values = list()
        for n, f in enumerate(self.get_fields()):
            if is_row(item):
                value = item[n] if n < len(item) or not skip_errors else None
            elif is_record(item):
                value = item.get(get_name(f))
            else:
                raise TypeError('Expected item as Row or Record, got {}'.format(item))
            if isinstance(f, (FieldInterface, AnyField)) or hasattr(f, 'format'):
                str_value = f.format(value, skip_errors=skip_errors)
            else:
                str_value = str(value)
            formatted_values.append(str_value)
        return delimiter.join(formatted_values)

    def simple_select_fields(self, fields: Iterable) -> Group:
        return FlatStruct(
            [self.get_field_description(f) for f in fields]
        )

    def get_fields_tuples(self) -> Iterator[tuple]:  # (name, type, caption, is_valid, group_caption)
        for f in self.get_fields():
            if isinstance(f, (FieldInterface, AnyField)):
                field_name = f.get_name()
                field_type_name = f.get_value_type_name()
                field_caption = f.get_caption() or EMPTY
                field_is_valid = str(f.is_valid())
                group_name = f.get_group_name()
                group_caption = f.get_group_caption()
            elif isinstance(f, tuple) and len(f) == 2:  # old-style FieldDescription
                field_name = f[0]
                field_type_name = ValueType(f[1]).get_name()
                field_is_valid = '?'
                field_caption, group_name, group_caption = EMPTY * 3
            else:
                field_name = str(f)
                field_type_name = ValueType.get_default()
                field_caption, field_is_valid, group_name, group_caption = EMPTY * 4
            str_field_is_valid = DICT_VALID_SIGN.get(field_is_valid, field_is_valid[:1])
            yield field_name, field_type_name, field_caption, str_field_is_valid, group_name, group_caption

    def get_field_description(self, field_name: Name, skip_missing: bool = False) -> Union[Field, FieldInterface]:
        field_position = self.get_field_position(field_name)
        if field_position is not None:
            return self.get_fields()[field_position]
        elif not skip_missing:
            message = 'Field {} not found (existing fields: {})'
            raise IndexError(message.format(field_name, ', '.join(self.get_field_names())))

    def get_struct_description_rows(self, include_header: bool = False) -> Iterator[tuple]:
        group_name = self.get_name()
        group_caption = self.get_caption()
        if include_header:
            yield TITLE_PREFIX, GROUP_TYPE_STR, group_name or EMPTY, group_caption, EMPTY
        for n, field_tuple in enumerate(self.get_fields_tuples()):
            f_name, f_type_name, f_caption, f_valid, group_name, group_caption = field_tuple
            yield n, f_type_name, f_name or EMPTY, f_caption, f_valid

    def get_input_fields(self) -> list:
        return self.get_fields()

    def to(self, field: Field):
        return self.map(lambda *a: tuple(a)).to(field)

    def get_group_header(self, name: Comment = AUTO, caption: Comment = AUTO, comment: Comment = None) -> Iterator[str]:
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

    @staticmethod
    def _get_describe_columns(example, with_lens: bool = True) -> tuple:
        if example:
            columns = ('valid', 'n', 'type_name', 'name', 'example', 'caption')
            lens = (1, 3, 8, 28, 14, 56)
        else:
            columns = ('valid', 'n', 'type_name', 'name', 'caption')
            lens = (1, 3, 8, 28, 72)
        if with_lens:
            return tuple(zip(columns, lens))
        else:
            return columns

    def get_struct_repr_lines(
            self,
            example: Optional[dict] = None,
            delimiter: str = REPR_DELIMITER,
            select_fields: Optional[Array] = None,
            count: Optional[int] = None
    ) -> Iterator[str]:
        columns, template = self._get_describe_template(example)
        separate_by_tabs = delimiter == '\t'
        yield '\t'.join(columns) if separate_by_tabs else template.format(*columns)
        for (n, type_name, name, caption, is_valid) in self.get_struct_description_rows(include_header=False):
            if type_name == GROUP_TYPE_STR:
                yield EMPTY
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
                row = map(str, row)
                yield '\t'.join(row) if separate_by_tabs else template.format(*row)
            if Auto.is_defined(count):
                if n >= count - 1:
                    break

    def get_struct_repr_records(
            self,
            example: Optional[dict] = None,
            select_fields: Optional[Array] = None,
            count: Optional[int] = None
    ) -> Iterator[dict]:
        value = None
        for (n, type_name, name, caption, is_valid) in self.get_struct_description_rows(include_header=False):
            if name in (select_fields or []):
                is_valid = '>' if is_valid == '.' else str(is_valid).upper()
            if example:
                value = str(example.get(name))
            yield dict(n=n, name=name, type_name=type_name, example=value, valid=is_valid, caption=caption)
            if Auto.is_defined(count):
                if n >= count - 1:
                    break

    def get_data_description(
            self,
            count: AutoCount = AUTO,
            title: Optional[str] = 'Columns:',
            example: Optional[dict] = None,
            select_fields: Optional[Array] = None,
            max_len: int = JUPYTER_LINE_LEN,
            delimiter: str = REPR_DELIMITER,
    ) -> Generator:
        struct_description_lines = self.get_struct_repr_lines(
            example=example, delimiter=delimiter, select_fields=select_fields,
            count=count,
        )
        for line in struct_description_lines:
            yield line[:max_len]

    def display_data_sheet(
            self,
            count: AutoCount = AUTO,
            title: Optional[str] = 'Columns',
            example: Optional[dict] = None,
            select_fields: Optional[Array] = None,
    ) -> None:
        display = self.get_display()
        columns = self._get_describe_columns(example, with_lens=True)
        records = self.get_struct_repr_records(example=example, select_fields=select_fields, count=count)
        if title:
            display.display_paragraph(title, level=3)
        return display.display_sheet(records, columns=columns, count=count)

    def get_dataframe(self) -> DataFrame:
        data = self.get_struct_description_rows(include_header=True)
        columns = ('n', 'type', 'name', 'caption', 'valid')
        return DataFrame(data, columns=columns)

    def show(self, count: Optional[int] = None) -> Optional[DataFrame]:
        return self.describe()

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

    def __len__(self):
        return self.get_count()


AnyField.set_struct_builder(FlatStruct)
