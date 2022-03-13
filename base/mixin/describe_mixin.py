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
DEFAULT_ROWS_COUNT = 10


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
            dimensions_repr.append(len_repr)
        if column_repr:
            dimensions_repr.append(column_repr)
        return ', '.join(dimensions_repr)

    def get_one_line_repr(
            self,
            str_meta: Union[str, Auto, None] = AUTO,
            max_len: int = JUPYTER_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        template = '{cls}({meta})'
        class_name = self.__class__.__name__
        str_meta = Auto.delayed_acquire(str_meta, self.get_str_meta)
        one_line_repr = template.format(cls=class_name, meta=str_meta)
        full_line_len = len(one_line_repr)
        if full_line_len > max_len:
            exceeded_len = full_line_len + len(crop) - max_len
            str_meta = str_meta[:-exceeded_len]
            one_line_repr = template.format(cls=class_name, meta=str_meta + crop)
        return one_line_repr

    def get_str_headers(self) -> Generator:
        yield self.get_one_line_repr()

    def _get_init_defaults(self) -> dict:
        args = getfullargspec(self.__init__).args
        defaults = getfullargspec(self.__init__).defaults
        no_default_count = len(args) - len(defaults)
        return dict(zip(args[no_default_count:], defaults))

    def _get_init_types(self) -> dict:
        return getfullargspec(self.__init__).annotations

    def has_data(self) -> bool:
        if hasattr(self, 'get_data'):
            return bool(self.get_data())
        else:
            return False

    @staticmethod
    def _get_value_repr(value: Value, default: str = '-') -> str:
        if isinstance(value, Callable):
            return get_name(value, or_callable=False)
        elif value is not None:
            return repr(value)
        else:
            return default

    def get_meta_defaults(self) -> Generator:
        meta = self.get_meta()
        args = getfullargspec(self.__init__).args
        defaults = getfullargspec(self.__init__).defaults
        for k, v in zip(args, defaults):
            if k in meta:
                yield k, v
        for k in meta:
            if k not in args:
                yield None

    def get_meta_records(self) -> Generator:
        init_defaults = self._get_init_defaults()
        for key, value in self.get_meta_items():
            actual_type = type(value).__name__
            expected_type = self._get_init_types().get(key)
            if hasattr(expected_type, '__name__'):
                expected_type = expected_type.__name__
            else:
                expected_type = str(expected_type).replace('typing.', '')
            default = init_defaults.get(key)
            yield dict(
                key=key,
                value=self._get_value_repr(value),
                default=self._get_value_repr(default),
                actual_type=actual_type,
                expected_type=expected_type or '-',
                defined='-' if value == default else '+' if Auto.is_defined(value) else 'x',
            )

    def get_data_description(
            self,
            count: int = DEFAULT_ROWS_COUNT,
            title: Optional[str] = 'Data:',
            max_len: int = JUPYTER_LINE_LEN,
    ) -> Generator:
        if title:
            yield title
        if hasattr(self, 'get_data_caption'):
            yield self.get_data_caption()
        if hasattr(self, 'get_data'):
            data = self.get_data()
            if data:
                shape_repr = self.get_shape_repr()
                if shape_repr:
                    yield 'First {count} data items from {shape}:'.format(count=count, shape=shape_repr)
                if isinstance(data, dict):
                    records = map(
                        lambda i: dict(key=i[0], value=i[1], defined='+' if Auto.is_defined(i[1]) else '-'),
                        data.items(),
                    )
                    yield from self._get_columnar_lines(
                        records, columns=DICT_DESCRIPTION_COLUMNS, count=count, max_len=max_len,
                    )
                elif isinstance(data, Iterable):
                    for n, item in enumerate(data):
                        if n >= count:
                            break
                        line = '    - ' + str(item)
                        yield line[:max_len]
                elif isinstance(data, DescribeMixin) or hasattr(data, 'get_meta_description'):
                    for line in data.get_meta_description():
                        yield line
                else:
                    line = str(data)
                    yield line[:max_len]
            else:
                yield '(data attribute is empty)'
        else:
            yield '(data attribute not found)'

    def get_meta_description(
            self,
            with_title: bool = True,
            with_summary: bool = True,
            prefix: str = PREFIX_VALUE,
            delimiter: str = COLUMN_DELIMITER,
    ) -> Generator:
        if with_summary:
            yield '{name} has {count} attributes in meta-data:'.format(name=repr(self), count=len(self.get_meta()))
        yield from self._get_columnar_lines(
            records=self.get_meta_records(),
            columns=META_DESCRIPTION_COLUMNS,
            with_title=with_title,
            prefix=prefix,
            delimiter=delimiter,
        )

    def describe(
            self,
            show_header: bool = True,
            count: AutoCount = AUTO,
            depth: int = 1,
            output: AutoOutput = AUTO,
            as_dataframe: bool = Auto,
            **kwargs
    ):
        as_dataframe = Auto.acquire(as_dataframe, hasattr(self, 'show') or hasattr(self, 'show_example'))
        show_meta = show_header or not self.has_data()
        if show_header:
            for line in self.get_str_headers():
                self.output_line(line, output=output)
        if show_meta:
            for line in self.get_meta_description():
                self.output_line(line, output=output)
        if self.has_data():
            if not as_dataframe:
                self.output_blank_line(output=output)
                for line in self.get_data_description(count=count, **kwargs):
                    self.output_line(line, output=output)
        elif depth > 0:
            for attribute, value in self.get_meta_items():
                if isinstance(value, DescribeMixin) or hasattr(value, 'describe'):
                    self.output_blank_line(output=output)
                    self.output_line('{attribute}:'.format(attribute=attribute), output=output)
                    value.describe(show_header=False, depth=depth - 1, output=output)
        if self.has_data() and as_dataframe:
            if hasattr(self, 'show_example'):
                return self.show_example(count=count, **kwargs)
            elif hasattr(self, 'show'):
                return self.show(count=count, **kwargs)
            else:
                raise AttributeError('{} does not support dataframe'.format(self))
