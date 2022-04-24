from abc import ABC
from typing import Optional, Callable, Iterable, Generator, Sequence, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoCount, Class
    from base.functions.arguments import get_name, get_value
    from base.constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from base.interfaces.line_output_interface import LineOutputInterface, AutoOutput, LoggingLevel
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoCount, Class
    from ..functions.arguments import get_name, get_value
    from ..constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from ..interfaces.line_output_interface import LineOutputInterface, AutoOutput, LoggingLevel

DEFAULT_ROWS_COUNT = 10
DEFAULT_INT_WIDTH, DEFAULT_FLOAT_WIDTH = 7, 12
PREFIX_FIELD = 'prefix'


class LineOutputMixin(LineOutputInterface, ABC):
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
        logger_kwargs = dict(stacklevel=None)
        if isinstance(output, LoggingLevel):
            logger_kwargs['level'] = output
            output = AUTO
        if Auto.is_auto(output):
            if hasattr(self, 'log'):
                return self.log(msg=line, **logger_kwargs)
            else:
                output = self.get_output()
        if isinstance(output, Callable):
            return output(line)
        elif output:
            if hasattr(output, 'output_line'):
                try:
                    return output.output_line(line=line)
                except TypeError as e:
                    raise TypeError(f'{output}.output_line({line}): {e}')
                return output.output_line(line)
            elif hasattr(output, 'log'):
                return output.log(msg=line, **logger_kwargs)
            else:
                raise TypeError('Expected Output, Logger or Auto, got {}'.format(output))

    def output_blank_line(self, output: AutoOutput = AUTO) -> None:
        self.output_line(EMPTY, output=output)

    @classmethod
    def _get_formatter(cls, columns: Sequence, delimiter: str = REPR_DELIMITER) -> str:
        meta_description_placeholders = list()
        for name, size in zip(cls._get_column_names(columns), cls._get_column_lens(columns)):
            if size is None:
                formatter = name
            elif size:
                formatter = '{name}:{size}'.format(name=name, size=size)
            else:
                formatter = EMPTY
            meta_description_placeholders.append('{open}{f}{close}'.format(open='{', f=formatter, close='}'))
        return delimiter.join(meta_description_placeholders)

    @staticmethod
    def _get_column_names(columns: Iterable, ex: Union[str, Sequence, None] = None) -> Generator:
        if ex is None:
            ex = []
        elif isinstance(ex, str):
            ex = [ex]
        for c in columns:
            if c in ex:
                yield ''
            elif isinstance(c, (int, str)):
                yield c
            elif isinstance(c, Sequence):
                yield c[0]
            else:
                raise get_name(c)

    @staticmethod
    def _get_column_lens(columns: Iterable, max_len: Optional[int] = None) -> Generator:
        for c in columns:
            if isinstance(c, (int, str)):
                yield max_len
            elif isinstance(c, Sequence):
                if len(c) > 1:
                    if isinstance(c[1], int):
                        yield c[1]
                    elif c[1] == int:
                        yield DEFAULT_INT_WIDTH
                    elif c[1] == float:
                        yield DEFAULT_FLOAT_WIDTH
                    else:  # c == str
                        yield max_len
                else:
                    yield max_len
            elif hasattr(c, 'get_max_len'):
                yield c.get_max_len()
            elif hasattr(c, 'get_repr'):
                yield c.get_repr().get_max_len()
            else:
                yield max_len

    @classmethod
    def _get_cropped_record(
            cls,
            item: Union[dict, Iterable],
            columns: Sequence,
            max_len: int = DEFAULT_LINE_LEN,
            ex: Union[str, Sequence, None] = None,
    ) -> dict:
        if ex is None:
            ex = []
        elif isinstance(ex, str):
            ex = [ex]
        names = list(cls._get_column_names(columns, ex=ex))
        lens = cls._get_column_lens(columns, max_len=max_len)
        if isinstance(item, dict):
            values = [str(get_value(item.get(k))) if k not in ex else '' for k in names]
        else:
            values = [str(v) if k not in ex else '' for k, v in zip(names, item)]
        return {c: str(v)[:s] for c, v, s in zip(names, values, lens)}

    @classmethod
    def _get_columnar_lines(
            cls,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            prefix: str = SMALL_INDENT,
            delimiter: str = REPR_DELIMITER,
            max_len: int = DEFAULT_LINE_LEN,
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

    def output_table(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
    ) -> None:
        columnar_lines = self._get_columnar_lines(records, columns=columns, count=count, with_title=with_title)
        for line in columnar_lines:
            self.output_line(line)
