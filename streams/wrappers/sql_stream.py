from enum import Enum
from typing import Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ContextInterface, LeafConnectorInterface, StreamInterface,
        ConnType, LoggingLevel, StreamType, Stream, Item, Name, Links, Array, ARRAY_TYPES,
        AutoContext, AutoName, AutoBool, Auto, AUTO,
    )
    from functions.primary.text import remove_extra_spaces
    from content.fields.abstract_field import AbstractField
    from streams.abstract.wrapper_stream import WrapperStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ContextInterface, LeafConnectorInterface, StreamInterface, Stream, Item,
        ConnType, LoggingLevel, StreamType, Stream, Item, Name, Links, Array, ARRAY_TYPES,
        AutoContext, AutoName, AutoBool, Auto, AUTO,
    )
    from ...functions.primary.text import remove_extra_spaces
    from ...content.fields.abstract_field import AbstractField
    from ..abstract.wrapper_stream import WrapperStream

Native = WrapperStream
TableOrQuery = Union[LeafConnectorInterface, StreamInterface, None]

DICT_FUNC_NAMES = dict(len='COUNT')


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
        super().__init__(
            name=name,
            data=data or dict(),
            source=source,
            context=context,
            check=False,
        )

    def get_source_table(self):
        source = self.get_source()
        if source.get_conn_type() == ConnType.Table:
            return source
        else:
            return source.get_source_table()

    def get_database(self):
        table = self.get_source_table()
        assert table.get_conn_type() == ConnType.Table
        return table.get_database()

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

    def add_expression_for(self, section: SqlSection, expression: Union[str, int, Array]) -> Native:
        if section == SqlSection.From:
            self.set_source(expression)
        self.get_expressions_for(section).append(expression)
        return self

    def get_select_lines(self) -> Iterable:
        descriptions = self.get_expressions_for(SqlSection.Select)
        if not descriptions:
            yield '*'
        for desc in descriptions:
            if isinstance(desc, str):
                yield desc
            elif isinstance(desc, AbstractField):
                if hasattr(desc, 'get_sql_expression'):
                    yield desc.get_sql_expression()
                else:
                    yield desc.get_name()
            elif isinstance(desc, ARRAY_TYPES):
                target_field = desc[0]
                expression = desc[1:]
                if len(expression) == 1:
                    source_field = expression[0]
                    yield '{} AS {}'.format(source_field, target_field)
                elif len(expression) == 2:
                    if isinstance(expression[0], Callable):
                        function, source_field = expression
                    else:
                        source_field, function = expression
                    function_name = function.__name__
                    sql_function_name = DICT_FUNC_NAMES.get(function_name, function_name)
                    yield '{}({}) AS {}'.format(sql_function_name, source_field, target_field)
                else:
                    raise NotImplementedError('got {}'.format(desc))
            else:
                raise ValueError('expected field name or tuple, got {}'.format(desc))

    def get_where_lines(self) -> Iterable:
        for description in self.get_expressions_for(SqlSection.Where):
            if isinstance(description, str):
                yield '{} <> 0 AND {} NOT NULL'.format(description, description)
            elif isinstance(description, AbstractField):
                if hasattr(description, 'get_sql_expression'):
                    yield description.get_sql_expression()
                else:
                    yield '{} <> 0 AND {} NOT NULL'.format(description.get_name(), description.get_name())
            elif isinstance(description, ARRAY_TYPES):
                target_field = description[0]
                expression = description[1:]
                if len(expression) == 1:
                    value = expression[0]
                    yield '{} = {}'.format(target_field, value)
                if len(expression) == 2:
                    raise NotImplemented('got {}'.format(description))
            else:
                raise ValueError('expected field name or tuple, got {}'.format(description))

    def get_from_lines(self) -> Iterable:
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
                yield ')'
            else:
                raise ValueError('from-section data must be Table or str, got {}'.format(from_obj))
        else:
            yield from from_section

    def get_groupby_lines(self) -> Iterable:
        yield from self.get_expressions_for(SqlSection.GroupBy)

    def get_orderby_lines(self) -> Iterable:
        yield from self.get_expressions_for(SqlSection.OrderBy)

    def get_limit_lines(self) -> Iterable:
        yield from self.get_expressions_for(SqlSection.Limit)

    def get_section_lines(self, section: SqlSection) -> Iterable:
        method_name = 'get_{}_lines'.format(section.name.lower())
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
                delimiter = ''
            else:
                delimiter = ', '
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
        return self.__class__(source=self, **kwargs)

    def select(self, *fields, **expressions) -> Native:
        select_section = self.get_expressions_for(SqlSection.Select)
        if select_section:
            return self.new().select(*fields, **expressions)
        else:
            list_expressions = list(fields)
            for target, source in expressions.items():
                if isinstance(source, ARRAY_TYPES):
                    list_expressions.append((target, *source))
                else:
                    list_expressions.append((target, source))
            for expression in list_expressions:
                self.add_expression_for(SqlSection.Select, expression)
            return self

    def filter(self, *fields, **expressions) -> Native:
        if self.has_any_section():
            return self.new().filter(*fields, **expressions)
        else:
            list_expressions = list(fields) + [(field, value) for field, value in expressions.items()]
            for expressions in list_expressions:
                self.add_expression_for(SqlSection.Where, expressions)
            return self

    def group_by(self, *fields) -> Native:
        select_section = self.get_expressions_for(SqlSection.Select)
        groupby_section = self.get_expressions_for(SqlSection.GroupBy)
        if select_section or groupby_section:
            return self.new().group_by(*fields)
        else:
            for f in fields:
                self.add_expression_for(SqlSection.GroupBy, f)
            return self

    def sort(self, *fields) -> Native:
        select_section = self.get_expressions_for(SqlSection.Select)
        groupby_section = self.get_expressions_for(SqlSection.GroupBy)
        if select_section or groupby_section:
            return self.new().sort(*fields)
        else:
            for f in fields:
                self.add_expression_for(SqlSection.OrderBy, f)
            return self

    def take(self, count: int) -> Native:
        self.add_expression_for(SqlSection.Limit, count)
        return self

    def get_count(self) -> int:
        transform = self.select(cnt=(len, '*'))
        assert isinstance(transform, SqlStream)
        data = transform.execute_query()
        count = list(data)[0]
        return count

    def get_items(self) -> Iterable:
        return self.execute_query()

    def map(self, function: Callable) -> Native:
        raise NotImplementedError

    def skip(self, count: int) -> Native:
        raise NotImplementedError

    def collect(self, stream_type: StreamType = StreamType.RecordStream) -> Stream:
        stream_class = stream_type.get_class()
        return stream_class(self.execute_query())

    def get_demo_example(self, count: int = 10) -> Iterable:
        return self.copy().take(count).collect().get_items()

    def one(self) -> Stream:
        return self.copy().take(1).collect()

    def get_one_item(self) -> Item:
        items = self.one().get_items()
        return list(items)[0]

    def copy(self) -> Native:
        data = self._data.copy()
        return self.make_new(data)

    def get_one_line_representation(self) -> str:
        template = '{cls}({name}, {data})'
        message = template.format(cls=self.__class__.__name__, name=self.get_name(), data=self.get_data())
        return message

    def get_description_lines(self) -> Generator:
        yield self.get_one_line_representation()
        yield '\nGenerated SQL query:\n'
        yield from self.get_query_lines()

    def describe(self) -> Native:
        for line in self.get_description_lines():
            self.log(msg=line, level=LoggingLevel.Info)
        return self
