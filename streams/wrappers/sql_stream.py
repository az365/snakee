from enum import Enum
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.context_interface import ContextInterface
    from streams.abstract.wrapper_stream import WrapperStream
    from connectors import connector_classes as ct
    from fields.abstract_field import AbstractField
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.context_interface import ContextInterface
    from ..abstract.wrapper_stream import WrapperStream
    from ...connectors import connector_classes as ct
    from ...fields.abstract_field import AbstractField

Native = WrapperStream
Context = Union[ContextInterface, arg.DefaultArgument, None]
Field = AbstractField
Array = Union[list, tuple]

ARRAY_TYPES = list, tuple
DEFAULT_NAME = 'sql'
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
            name: Optional[str] = None,
            data: Optional[dict] = None,
            source=None,
            context: Context = arg.DEFAULT,
    ):
        super().__init__(
            name=name or DEFAULT_NAME,
            data=data or dict(),
            source=source,
            context=context,
        )

    def get_source_table(self):
        source = self.get_source()
        if isinstance(source, ct.Table):
            return source
        else:
            return source.get_source_table()

    def get_database(self):
        table = self.get_source_table()
        assert isinstance(table, ct.Table)
        return table.get_database()

    def execute_query(self) -> Iterable:
        db = self.get_database()
        assert isinstance(db, ct.AbstractDatabase)
        return db.execute(self.get_query())

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
            elif isinstance(desc, Field):
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
            elif isinstance(description, Field):
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

    def get_query(self) -> str:
        return '\n'.join(list(self.get_query_lines()))

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
        return self.__class__(
            source=self,
            **kwargs
        )

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

    def get_demo_example(self, *args, **kwargs):
        raise NotImplementedError

    def map(self, function: Callable) -> Native:
        raise NotImplementedError

    def skip(self, count: int) -> Native:
        raise NotImplementedError

    def get_child(self, name):
        pass

    def add_child(self, name):
        pass
