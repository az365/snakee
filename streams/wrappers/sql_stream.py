from enum import Enum
from typing import Optional, Callable, Iterable, Generator, Sequence, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ContextInterface, LeafConnectorInterface, StreamInterface, StructInterface,
        ConnType, LoggingLevel, ItemType, StreamType, Stream, RegularStream,
        AutoContext, AutoStreamType, AutoName, AutoBool, Auto, AUTO,
        Item, Name, Links, Columns, OptionalFields, Array,
    )
    from base.functions.arguments import get_names, get_name, get_generated_name, get_str_from_args_kwargs
    from base.constants.chars import EMPTY, ALL, CROP_SUFFIX, ITEMS_DELIMITER
    from functions.primary.text import remove_extra_spaces
    from content.fields.abstract_field import AbstractField
    from content.selection.abstract_expression import AbstractDescription
    from content.selection.concrete_expression import AliasDescription
    from content.struct.flat_struct import FlatStruct
    from streams.abstract.wrapper_stream import WrapperStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ContextInterface, LeafConnectorInterface, StreamInterface, StructInterface,
        ConnType, LoggingLevel, ItemType, StreamType, Stream, RegularStream,
        AutoContext, AutoStreamType, AutoName, AutoBool, Auto, AUTO,
        Item, Name, Links, Columns, OptionalFields, Array,
    )
    from ...base.functions.arguments import get_names, get_name, get_generated_name, get_str_from_args_kwargs
    from ...base.constants.chars import EMPTY, ALL, CROP_SUFFIX, ITEMS_DELIMITER
    from ...functions.primary.text import remove_extra_spaces
    from ...content.fields.abstract_field import AbstractField
    from ...content.selection.abstract_expression import AbstractDescription
    from ...content.selection.concrete_expression import AliasDescription
    from ...content.struct.flat_struct import FlatStruct
    from ..abstract.wrapper_stream import WrapperStream

Native = WrapperStream
TableOrQuery = Union[LeafConnectorInterface, StreamInterface, None]

IS_DEFINED = '{field} <> 0 and {field} NOT NULL'
MSG_NOT_IMPL = '{method}() operation is not defined for SqlStream, try to use .to_record_stream().{method}() instead'
DICT_FUNC_NAMES = dict(len='COUNT')
DICT_TYPE_NAMES = dict(int='integer')


class SqlSection(Enum):
    Select = 'SELECT'
    From = 'FROM'
    Where = 'WHERE'
    GroupBy = 'GROUP BY'
    OrderBy = 'ORDER BY'
    Limit = 'LIMIT'


SECTIONS_ORDER = (
    SqlSection.Select, SqlSection.From, SqlSection.Where, SqlSection.GroupBy, SqlSection.OrderBy, SqlSection.Limit,
)


class SqlStream(WrapperStream):
    def __init__(
            self,
            data: Links = None,
            name: AutoName = AUTO,
            source: TableOrQuery = None,
            context: AutoContext = AUTO,
    ):
        if not Auto.is_defined(data):
            data = dict()
        super().__init__(
            name=name,
            data=data,
            source=source,
            context=context,
            check=False,
        )

    @staticmethod
    def get_item_type() -> ItemType:
        return ItemType.Row

    def get_source_table(self) -> LeafConnectorInterface:
        source = self.get_source()
        if isinstance(source, LeafConnectorInterface):  # source.get_type() == ConnType.Table
            return source
        elif isinstance(source, SqlStream) or hasattr(source, 'get_source_table'):
            return source.get_source_table()
        else:
            raise TypeError('Expected source as Table or SqlStream, got {}'.format(source))

    def get_database(self):
        table = self.get_source_table()
        if hasattr(table, 'get_database'):
            return table.get_database()
        else:
            raise TypeError('Expected source as Table or SqlStream, got {}'.format(table))

    def close(self) -> int:
        return self.get_database().close()

    def execute_query(self, verbose: AutoBool = AUTO) -> Iterable:
        db = self.get_database()
        return db.execute(self.get_query(), get_data=True, verbose=verbose)

    def get_expressions_for(self, section: SqlSection) -> list:
        if section == SqlSection.From:
            return [self.get_source()]
        children = self.get_data()
        if not children.get(section):
            children[section] = list()
        return children[section]

    def add_expression_for(
            self,
            section: SqlSection,
            expression: Union[str, int, Array, LeafConnectorInterface, Native],
            inplace: bool = True,
    ) -> Native:
        if inplace:
            stream = self
        else:
            stream = self.copy()
        if section == SqlSection.From:
            assert isinstance(section, (LeafConnectorInterface, SqlStream))
            stream.set_source(expression, inplace=True)
        stream.get_expressions_for(section).append(expression)
        return stream

    def get_select_lines(self) -> Generator:
        descriptions = self.get_expressions_for(SqlSection.Select)
        if not descriptions:
            yield ALL
        for desc in descriptions:
            if isinstance(desc, str):
                yield desc
            elif isinstance(desc, AbstractField):
                if hasattr(desc, 'get_sql_expression'):
                    yield desc.get_sql_expression()
                else:
                    yield desc.get_name()
            elif isinstance(desc, AbstractDescription):
                yield desc.get_sql_expression()
            elif isinstance(desc, Sequence):
                target_field = desc[0]
                expression = desc[1:]
                if len(expression) == 1:
                    source_field = expression[0]
                    yield '{} AS {}'.format(source_field, target_field)
                elif len(expression) == 2:
                    if isinstance(expression[0], Callable):
                        function, source_field = expression
                    elif isinstance(expression[-1], Callable):
                        source_field, function = expression
                    else:
                        msg = 'Expected tuple (function, *fields) or (*fields, function), got {}'
                        raise ValueError(msg.format(expression))
                    if hasattr(function, 'get_sql_expr'):
                        sql_function_expr = function.get_sql_expr(source_field)
                    else:
                        function_name = function.__name__
                        sql_type_name = DICT_TYPE_NAMES.get(function_name)
                        if sql_type_name:
                            sql_function_expr = '{}::{}'.format(source_field, sql_type_name)
                        else:
                            sql_function_name = DICT_FUNC_NAMES.get(function_name)
                            if not sql_function_name:
                                self.get_logger().warning('Unsupported function call: {}'.format(function_name))
                                sql_function_name = function_name
                            sql_function_expr = '{}({})'.format(sql_function_name, source_field)
                    yield '{} AS {}'.format(sql_function_expr, target_field)
                else:
                    raise NotImplementedError('got {}'.format(desc))
            else:
                raise ValueError('expected field name or tuple, got {}'.format(desc))

    def get_where_lines(self) -> Generator:
        for description in self.get_expressions_for(SqlSection.Where):
            if isinstance(description, str):
                yield IS_DEFINED.format(field=description)
            elif isinstance(description, AbstractField):
                if hasattr(description, 'get_sql_expression'):
                    yield description.get_sql_expression()
                else:
                    yield IS_DEFINED.format(field=description.get_name())
            elif isinstance(description, Sequence):
                target_field = description[0]
                expression = description[1:]
                if len(expression) == 1:
                    value = expression[0]
                    if isinstance(value, str):
                        yield "{} = '{}'".format(target_field, value)
                    elif isinstance(value, Callable):
                        func = value
                        if hasattr(func, 'get_sql_expr'):
                            yield func.get_sql_expr(target_field)
                        else:
                            func_name = func.__name__
                            sql_func_name = DICT_FUNC_NAMES.get(func_name)
                            if not sql_func_name:
                                self.get_logger().warning('Unsupported function call: {}'.format(func_name))
                                sql_func_name = func_name
                            yield '{}({})'.format(sql_func_name, target_field)
                    else:
                        yield '{} = {}'.format(target_field, value)
                if len(expression) == 2:
                    raise NotImplemented('got {}'.format(description))
            else:
                raise ValueError('expected field name or tuple, got {}'.format(description))

    def get_from_lines(self) -> Generator:
        from_section = list(self.get_expressions_for(SqlSection.From))
        if len(from_section) == 1:
            from_obj = from_section[0]
            if isinstance(from_obj, str):
                yield from_obj
            elif hasattr(from_obj, 'get_path'):  # isinstance(from_obj, Table):
                yield from_obj.get_path()
            elif hasattr(from_obj, 'get_query_lines'):  # isinstance(from_obj, SqlTransform)
                yield '('
                yield from from_obj.get_query_lines(finish=False)
                yield ') AS {}'.format(get_generated_name('subquery', include_random=True, include_datetime=False))
            else:
                raise ValueError('from-section data must be Table or str, got {}'.format(from_obj))
        else:
            yield from from_section

    def get_groupby_lines(self) -> Generator:
        for f in self.get_expressions_for(SqlSection.GroupBy):
            yield get_name(f)

    def get_orderby_lines(self) -> Generator:
        for f in self.get_expressions_for(SqlSection.OrderBy):
            yield get_name(f)

    def get_limit_lines(self) -> Generator:
        yield from self.get_expressions_for(SqlSection.Limit)

    def get_section_lines(self, section: SqlSection) -> Iterable:
        method_name = 'get_{}_lines'.format(get_name(section).lower())
        method = self.__getattribute__(method_name)
        yield from method()

    def get_one_line_query(self, finish: bool = True) -> str:
        query = self.get_query(finish=finish)
        return remove_extra_spaces(query)

    def get_query(self, finish: bool = True) -> str:
        return '\n'.join(list(self.get_query_lines(finish=finish)))

    def get_query_lines(self, finish: bool = True) -> Iterable:
        for section in SECTIONS_ORDER:
            lines = self.get_section_lines(section)
            yield from self._format_section_lines(section, lines)
        if finish:
            yield ';'

    @staticmethod
    def _format_section_lines(section: SqlSection, lines: Iterable) -> Iterable:
        lines = list(lines)
        if lines:
            yield section.value
            if section == SqlSection.Where:
                delimiter = '\n    AND '
            elif section == SqlSection.From:
                delimiter = EMPTY
            else:
                delimiter = ITEMS_DELIMITER
            for n, line in enumerate(lines):
                is_last = n == len(lines) - 1
                template = '    {}' if is_last else '    {}' + delimiter
                yield template.format(line)

    def has_any_section(self) -> bool:
        for section in SECTIONS_ORDER:
            if section != SqlSection.From:
                if self.get_expressions_for(section):
                    return True
        return False

    def new(self, **kwargs):
        if 'source' not in kwargs:
            kwargs['source'] = self
        return self.__class__(**kwargs)

    def copy(self) -> Native:
        data = self._data.copy()
        stream = self.make_new(data)
        return self._assume_native(stream)

    def select(self, *fields, **expressions) -> Native:
        select_section = self.get_expressions_for(SqlSection.Select)
        if select_section:
            return self.new().select(*fields, **expressions)
        else:
            stream = self.copy()
            assert isinstance(stream, SqlStream)
            list_expressions = list(fields)
            for target, source in expressions.items():
                if isinstance(source, Sequence):
                    list_expressions.append((target, *source))
                else:
                    list_expressions.append((target, source))
            for expression in list_expressions:
                stream.add_expression_for(SqlSection.Select, expression)
            return stream

    def filter(self, *fields, **expressions) -> Native:
        if self.has_any_section():
            return self.new().filter(*fields, **expressions)
        else:
            stream = self.copy()
            assert isinstance(stream, SqlStream)
            list_expressions = list(fields) + [(field, value) for field, value in expressions.items()]
            for expressions in list_expressions:
                stream.add_expression_for(SqlSection.Where, expressions)
            return stream

    def group_by(self, *fields, values: Optional[list] = None) -> Native:
        if values:
            columns = self.get_input_columns()
            assert min([c in columns for c in get_names(values)])
        select_section = self.get_expressions_for(SqlSection.Select)
        groupby_section = self.get_expressions_for(SqlSection.GroupBy)
        if select_section or groupby_section:
            stream = self.new().group_by(*fields)
        else:
            stream = self.copy()
            assert isinstance(stream, SqlStream)
            for f in fields:
                stream.add_expression_for(SqlSection.GroupBy, f)
        if values:
            assert isinstance(stream, SqlStream)
            stream = stream.select(*fields, *values)
        return stream

    def sort(self, *fields) -> Native:
        stream = self.copy()
        assert isinstance(stream, SqlStream)
        for f in fields:
            stream.add_expression_for(SqlSection.OrderBy, f)
        return stream

    def take(self, count: int) -> Native:
        return self.add_expression_for(SqlSection.Limit, count, inplace=False)

    def get_count(self) -> int:
        transform = self.select(cnt=(len, ALL))
        assert isinstance(transform, SqlStream)
        data = transform.execute_query()
        count = list(data)[0]
        return count

    def get_items(self) -> Iterable:
        return self.execute_query()

    def map(self, function: Callable) -> Native:
        raise NotImplementedError(MSG_NOT_IMPL.format(method='map'))

    def skip(self, count: int) -> Native:
        raise NotImplementedError(MSG_NOT_IMPL.format(method='map'))

    def get_source_table_struct(self) -> StructInterface:
        source = self.get_source_table().get_struct()
        return source

    def get_input_struct(self) -> StructInterface:
        source = self.get_source()
        assert isinstance(source, (SqlStream, LeafConnectorInterface)) or hasattr(source, 'get_struct')
        return source.get_struct()

    def get_output_struct(self) -> StructInterface:
        input_struct = self.get_input_struct()
        output_columns = self.get_output_columns()
        types = {f: t for f, t in input_struct.get_types_dict().items() if f in output_columns}
        struct = FlatStruct(output_columns).set_types(types)
        assert isinstance(struct, FlatStruct)
        struct.validate_about(input_struct, ignore_moved=True)
        return struct

    def get_input_columns(self) -> Columns:
        source = self.get_source()
        assert isinstance(source, (SqlStream, LeafConnectorInterface)) or hasattr(source, 'get_columns')
        return source.get_columns()

    def get_output_columns(self) -> Columns:
        select_expressions = self.get_expressions_for(SqlSection.Select)
        if select_expressions:
            columns = list()
            for i in select_expressions:
                if isinstance(i, AbstractDescription):
                    columns.append(i.get_target_field_name())
                elif isinstance(i, AbstractField):
                    columns.append(i.get_name())
                elif len(i) == 1 or isinstance(i, str):
                    if i == ALL or i[0] == ALL:
                        for source_column in self.get_input_columns():
                            columns.append(source_column)
                    else:
                        columns.append(i)
                elif len(i) > 1:
                    columns.append(i[0])
                else:
                    raise ValueError(i)
            return columns
        else:
            return list(self.get_input_columns())

    def get_columns(self) -> Columns:
        return self.get_output_columns()

    def get_struct(self) -> StructInterface:
        return self.get_output_struct()

    def get_rows(self, verbose: bool = True) -> Iterable:
        return self.execute_query(verbose=verbose)

    def get_records(self) -> Iterable:
        columns = self.get_output_columns()
        return map(lambda r: dict(zip(columns, r)), self.get_rows())

    def to_row_stream(self) -> Stream:
        return self.to_stream(self.get_rows(), stream_type=StreamType.RowStream)

    def to_record_stream(self) -> Stream:
        return self.to_stream(self.get_records(), stream_type=StreamType.RecordStream)

    def to_stream(
            self,
            data: Optional[Iterable] = None,
            stream_type: AutoStreamType = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Union[RegularStream, Native]:
        stream_type = Auto.acquire(stream_type, self.get_stream_type())
        if data:
            stream_class = stream_type.get_class()
            meta = self.get_compatible_meta(stream_class, ex=ex)
            meta.update(kwargs)
            if 'count' not in meta:
                meta['count'] = self.get_count()
            if 'source' not in meta:
                meta['source'] = self.get_source()
            return stream_class(data, **meta)
        elif stream_type == StreamType.SqlStream:
            return self
        else:
            method_suffix = StreamType.of(stream_type).get_method_suffix()
            method_name = 'to_{}'.format(method_suffix)
            stream_method = self.__getattribute__(method_name)
            return stream_method()

    def collect(self, stream_type: StreamType = StreamType.RecordStream) -> Stream:
        stream = self.to_stream(stream_type=stream_type).collect()
        return self._assume_native(stream)

    def get_demo_example(self, count: int = 10) -> Iterable:
        stream = self.copy().take(count)
        assert isinstance(stream, SqlStream)
        return stream.collect().get_items()

    def one(self) -> Stream:
        stream = self.copy().take(1)
        assert isinstance(stream, SqlStream)
        return stream.collect()

    def get_one_item(self) -> Item:
        items = self.one().get_items()
        return list(items)[0]

    def get_stream_representation(self) -> str:
        source = self.get_source()
        if isinstance(source, SqlStream) or hasattr(source, 'get_stream_representation'):
            sm_repr = source.get_stream_representation()
        else:
            sm_repr = repr(source)
        filter_expressions = self.get_expressions_for(SqlSection.Where)
        if filter_expressions:
            str_filter_expressions = list()
            for i in filter_expressions:
                if isinstance(i, (AbstractDescription, AbstractField)) or hasattr(i, 'get_brief_repr'):
                    str_filter_expressions.append(i.get_brief_repr())
                elif isinstance(i, Sequence):
                    str_filter_expressions.append('{}={}'.format(get_name(i[0]), repr(i[1]) if len(i) == 2 else i[1:]))
            sm_repr += '.filter({})'.format(ITEMS_DELIMITER.join(str_filter_expressions))
        groupby_expressions = self.get_expressions_for(SqlSection.GroupBy)
        if groupby_expressions:
            sm_repr += '.group_by({})'.format(ITEMS_DELIMITER.join(get_names(groupby_expressions)))
        sort_expressions = self.get_expressions_for(SqlSection.OrderBy)
        if sort_expressions:
            sm_repr += '.sort({})'.format(ITEMS_DELIMITER.join(get_names(sort_expressions)))
        select_expressions = self.get_expressions_for(SqlSection.Select)
        if select_expressions:
            str_select_expressions = list()
            for i in select_expressions:
                if isinstance(i, (AbstractDescription, AbstractField)) or hasattr(i, 'get_brief_repr'):
                    str_select_expressions.append(i.get_brief_repr())
                elif isinstance(i, Sequence):
                    str_select_expressions.append('{}={}'.format(get_name(i[0]), repr(i[1]) if len(i) == 2 else i[1:]))
            sm_repr += '.select({})'.format(ITEMS_DELIMITER.join(str_select_expressions))
        return sm_repr

    def get_data_representation(self, max_len: int = 50) -> str:
        data_repr = self.get_stream_representation()
        if len(data_repr) > max_len:
            data_repr = data_repr[:max_len - len(CROP_SUFFIX)] + CROP_SUFFIX
        return data_repr

    def get_one_line_representation(self) -> str:
        message = '{}({}, {})'.format(self.__class__.__name__, self.get_name(), self.get_str_meta())
        return message

    def get_description_lines(self) -> Generator:
        yield repr(self)
        yield self.get_stream_representation()
        yield '\nGenerated SQL query:\n'
        yield from self.get_query_lines()
        yield '\nExpected output columns: {}'.format(self.get_output_columns())
        yield 'Expected input struct: {}'.format(self.get_source_table().get_struct())
        struct = self.get_struct()
        if hasattr(struct, 'get_struct_repr_lines'):
            yield from struct.get_struct_repr_lines(select_fields=self.get_output_columns())

    def describe(self) -> Native:
        for line in self.get_description_lines():
            self.log(msg=line, level=LoggingLevel.Info)
        return self

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def __str__(self):
        return self.get_one_line_representation()
