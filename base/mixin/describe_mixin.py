from abc import ABC
from typing import Optional, Iterable, Callable, Generator, Union
from inspect import getfullargspec

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoBool, AutoCount, Columns, Class, Value, Array, ARRAY_TYPES
    from base.functions.arguments import get_name, get_str_from_args_kwargs
    from base.interfaces.data_interface import SimpleDataInterfaced
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoBool, AutoCount, Columns, Class, Value, Array, ARRAY_TYPES
    from ..functions.arguments import get_name, get_str_from_args_kwargs
    from ..interfaces.data_interface import SimpleDataInterface

Native = SimpleDataInterface
LoggingLevel = int
AutoOutput = Union[Class, LoggingLevel, Callable, Auto]

PREFIX_FIELD = 'prefix'
PREFIX_VALUE = '  '
COLUMN_DELIMITER = ' '
CROP_SUFFIX = '..'
META_DESCRIPTION_COLUMNS = [
    (PREFIX_FIELD, 3), ('defined', 3),
    ('key', 20), ('value', 30), ('actual_type', 14), ('expected_type', 20), ('default', 20),
]
DICT_DESCRIPTION_COLUMNS = [(PREFIX_FIELD, 3), ('key', 20), 'value']
JUPYTER_LINE_LEN = 120
DEFAULT_SHOW_COUNT = 10


class DescribeMixin(ABC):
    def get_output(self, output: AutoOutput = AUTO) -> Optional[Class]:
        if Auto.is_defined(output) and not isinstance(output, LoggingLevel):
            return output
        if hasattr(self, 'get_logger'):
            logger = self.get_logger()
            if Auto.is_defined(logger):
                return logger
        elif Auto.is_auto(output):
            return print

    def output_line(self, line: str, output: AutoOutput = AUTO) -> None:
        if isinstance(output, LoggingLevel):
            logger_kwargs = dict(level=output)
            output = AUTO
        else:
            logger_kwargs = dict()
        if Auto.is_auto(output):
            if hasattr(self, 'log'):
                return self.log(msg=line, **logger_kwargs)
            else:
                output = self.get_output()
        if isinstance(output, Callable):
            return output(line)
        elif output:
            if hasattr(output, 'output_line'):
                return output.output_line(line)
            elif hasattr(output, 'log'):
                return output.log(msg=line, **logger_kwargs)
            else:
                raise TypeError('Expected Output, Logger or Auto, got {}'.format(output))

    def output_blank_line(self, output: AutoOutput = AUTO) -> None:
        self.output_line('', output=output)

    @classmethod
    def _get_formatter(cls, columns: Array, delimiter: str = ' ') -> str:
        meta_description_placeholders = list()
        for name, size in zip(cls._get_column_names(columns), cls._get_column_lens(columns)):
            if size is None:
                formatter = name
            elif size:
                formatter = '{name}:{size}'.format(name=name, size=size)
            else:
                formatter = ''
            meta_description_placeholders.append('{open}{f}{close}'.format(open='{', f=formatter, close='}'))
        return delimiter.join(meta_description_placeholders)

    @staticmethod
    def _get_column_names(columns: Iterable, ex: Union[str, Array, None] = None) -> Generator:
        if ex is None:
            ex = []
        elif isinstance(ex, str):
            ex = [ex]
        for c in columns:
            if c in ex:
                yield ''
            elif isinstance(c, (int, str)):
                yield c
            elif isinstance(c, ARRAY_TYPES):
                yield c[0]
            else:
                raise ValueError('Expected column description as str or tuple, got {}'.format(c))

    @staticmethod
    def _get_column_lens(columns: Iterable, max_len: Optional[int] = None) -> Generator:
        for c in columns:
            if isinstance(c, (int, str)):
                yield max_len
            elif isinstance(c, ARRAY_TYPES):
                yield c[1] if len(c) > 1 else c
            else:
                raise ValueError('Expected column description as str or tuple, got {}'.format(c))

    @classmethod
    def _get_cropped_record(
            cls,
            item: Union[dict, Iterable],
            columns: Array,
            max_len: int = JUPYTER_LINE_LEN,
            ex: Union[str, Array, None] = None,
    ) -> dict:
        if ex is None:
            ex = []
        elif isinstance(ex, str):
            ex = [ex]
        names = list(cls._get_column_names(columns, ex=ex))
        lens = cls._get_column_lens(columns, max_len=max_len)
        if isinstance(item, dict):
            values = [str(item.get(k)) if k not in ex else '' for k in names]
        else:
            values = [str(v) if k not in ex else '' for k, v in zip(names, item)]
        return {c: v[:s] for c, v, s in zip(names, values, lens)}

    @classmethod
    def _get_columnar_lines(
            cls,
            records: Iterable,
            columns: Array,
            count: AutoCount = None,
            with_title: bool = True,
            prefix: str = PREFIX_VALUE,
            delimiter: str = ' ',
            max_len: int = JUPYTER_LINE_LEN,
    ) -> Generator:
        count = Auto.acquire(count, DEFAULT_ROWS_COUNT)
        formatter = cls._get_formatter(columns=columns, delimiter=delimiter)
        if with_title:
            column_names = cls._get_column_names(columns, ex=PREFIX_FIELD)
            title_record = cls._get_cropped_record(column_names, columns=columns, max_len=max_len, ex=PREFIX_FIELD)
            yield formatter.format(**{k: v.upper() for k, v in title_record.items()})
        for n, r in enumerate(records):
            if count is not None and n >= count:
                break
            if prefix and PREFIX_FIELD not in r:
                r[PREFIX_FIELD] = prefix
            r = cls._get_cropped_record(r, columns=columns, max_len=max_len)
            yield formatter.format(**r)

    def get_brief_repr(self) -> str:
        return "{}('{}')".format(self.__class__.__name__, get_name(self, or_callable=False))

    def get_str_count(self, default: Optional[str] = '(iter)') -> Optional[str]:
        if hasattr(self, 'get_count'):
            count = self.get_count()
        else:
            count = None
        if Auto.is_defined(count):
            return str(count)
        else:
            return default

    def get_count_repr(self, default: str = '<iter>') -> str:
        count = self.get_str_count(default=default)
        if not Auto.is_defined(count):
            count = default
        return '{} items'.format(count)

    def get_shape_repr(self) -> str:
        len_repr = self.get_count_repr()
        if hasattr(self, 'get_column_repr'):
            column_repr = self.get_column_repr()
        else:
            column_repr = None
        dimensions_repr = list()
        if len_repr:
            dimensions_repr += len_repr
        if column_repr:
            dimensions_repr += column_repr
        return ', '.join(dimensions_repr)

    def get_one_line_repr(self) -> str:
        description_args = list()
        name = self.get_name()
        if name:
            description_args.append(name)
        if self.get_str_count(default=None) is not None:
            description_args.append(self.get_shape_repr())
        return '{}({})'.format(self.__class__, get_str_from_args_kwargs(*description_args))

    def get_detailed_repr(self) -> str:
        return '{}({})'.format(self.__class__.__name__, self.get_str_meta())

    def show(
            self,
            count: int = DEFAULT_SHOW_COUNT,
            message: Optional[str] = None,
            filters: Columns = None,
            columns: Columns = None,
            actualize: AutoBool = Auto,
            as_dataframe: AutoBool = Auto,
            **kwargs
    ):
        if hasattr(self, 'actualize'):
            if Auto.is_auto(actualize):
                self.actualize(if_outdated=True)
            elif actualize:
                self.actualize(if_outdated=False)
        return self.to_record_stream(message=message).show(
            count=count, as_dataframe=as_dataframe,
            filters=filters or list(), columns=columns,
        )

    def describe(
            self,
            *filter_args,
            count: Optional[int] = DEFAULT_SHOW_COUNT,
            columns: Optional[Array] = None,
            show_header: bool = True,
            struct_as_dataframe: bool = False,
            safe_filter: bool = True,
            actualize: AutoBool = AUTO,
            output: AutoOutput = AUTO,
            **filter_kwargs
    ):
        if show_header:
            for line in self.get_str_headers():
                self.output_line(line, output=output)
        example_item, example_stream, example_comment = dict(), None, ''
        if self.is_existing():
            if Auto.acquire(actualize, not self.is_actual()):
                self.actualize()
            if self.is_empty():
                message = '[EMPTY] file is empty, expected {} columns:'.format(self.get_column_count())
            else:
                message = self.get_validation_message()
                example_tuple = self._prepare_examples(safe_filter=safe_filter, filters=filter_args, **filter_kwargs)
                example_item, example_stream, example_comment = example_tuple
        else:
            message = '[NOT_EXISTS] file is not created yet, expected {} columns:'.format(self.get_column_count())
        if show_header:
            line = '{} {}'.format(self.get_datetime_str(), message)
            self.output_line(line, output=output)
            if self.get_invalid_fields_count():
                line = 'Invalid columns: {}'.format(get_str_from_args_kwargs(*self.get_invalid_columns()))
                self.output_line(line, output=output)
            self.output_blank_line(output=output)
        struct = self.get_struct()
        struct_dataframe = struct.describe(
            as_dataframe=struct_as_dataframe, example=example_item,
            output=output, comment=example_comment,
        )
        if struct_dataframe is not None:
            return struct_dataframe
        if example_stream and count:
            return self.show_example(
                count=count, example=example_stream,
                columns=columns, comment=example_comment,
            )
