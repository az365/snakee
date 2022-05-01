from typing import Optional, Iterable, Generator, Sequence, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoCount, Class
    from base.functions.arguments import get_name, get_value
    from base.constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from base.interfaces.display_interface import DisplayInterface, AutoStyle
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoCount, Class
    from ..functions.arguments import get_name, get_value
    from ..constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from ..interfaces.display_interface import DisplayInterface, AutoStyle

AutoDisplay = Union[Auto, DisplayInterface]
LoggingLevel = int

DEFAULT_ROWS_COUNT = 10
DEFAULT_INT_WIDTH, DEFAULT_FLOAT_WIDTH = 7, 12
PREFIX_FIELD = 'prefix'


class DefaultDisplay(DisplayInterface):
    def get_display(self, display: AutoDisplay = AUTO) -> DisplayInterface:
        if isinstance(display, DisplayInterface):
            return display
        elif not Auto.is_defined(display):
            if hasattr(self, '_display'):
                return self._display
            else:
                return self
        else:
            raise TypeError(f'expected Display, got {display}')

    def get_output(self, output: AutoDisplay = AUTO) -> DisplayInterface:
        return self.get_display(output)

    # @deprecated_with_alternative('display_paragraph()')
    def output_blank_line(self, output: AutoDisplay = AUTO) -> None:
        self.output_line(EMPTY, output=output)

    # @deprecated_with_alternative('add_to_paragraph()')
    def output_line(self, line: str, output: AutoDisplay = AUTO) -> None:
        return self.add_to_paragraph(line)

    def add_to_paragraph(self, text: str) -> None:
        self.display(text)

    def display_paragraph(
            self,
            paragraph: Optional[Iterable] = None,
            level: Optional[int] = None,
            style: AutoStyle = AUTO,
            output: AutoDisplay = AUTO,
    ) -> None:
        if paragraph:
            if isinstance(paragraph, str):
                return self.output_line(paragraph, output=output)
            elif isinstance(paragraph, Iterable):
                for line in paragraph:
                    return self.output_line(line, output=output)
            else:
                raise TypeError(f'Expected paragraph as Paragraph, str or Iterable, got {paragraph}')

    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            style: AutoStyle = AUTO,
            output: AutoDisplay = AUTO,
    ) -> None:
        columnar_lines = self._get_columnar_lines(records, columns=columns, count=count, with_title=with_title)
        for line in columnar_lines:
            self.output_line(line, output=output)

    def display_item(self, item, item_type='paragraph', output=AUTO, **kwargs) -> None:
        if hasattr(output, 'get_class'):
            output = output.get_class()
        elif Auto.is_auto(output):
            output = self.get_output()
        item_type_value = get_value(item_type)
        if item_type_value == 'sheet':
            return self.display_sheet(item, output=output, **kwargs)
        method_name = 'display_{item_type}'.format(item_type=item_type_value)
        method = getattr(self, method_name, self.display_paragraph())
        return method(item, output=output, **kwargs)

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

    @staticmethod
    def display(obj) -> None:
        print(obj)

    def __call__(self, obj) -> None:
        return self.display(obj)
