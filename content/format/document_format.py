from typing import Optional, Callable, Iterable, Iterator, Generator, Sequence, Union

try:  # Assume we're a submodule in a package.
    from interfaces import Item, ItemType, ContentType, Class, Count, AutoCount, Auto, AUTO
    from base.classes.display import DefaultDisplay, PREFIX_FIELD
    from base.mixin.display_mixin import DisplayMixin, AutoOutput, Class
    from utils.external import display, HTML, Markdown
    from utils.decorators import deprecated_with_alternative
    from content.format.text_format import TextFormat, Compress, DEFAULT_ENDING, DEFAULT_ENCODING
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Item, ItemType, ContentType, Class, Count, AutoCount, Auto, AUTO
    from ...base.classes.display import DefaultDisplay, PREFIX_FIELD
    from ...base.mixin.display_mixin import DisplayMixin, AutoOutput, Class
    from ...utils.external import display, HTML, Markdown
    from ...utils.decorators import deprecated_with_alternative
    from .text_format import TextFormat, Compress, DEFAULT_ENDING, DEFAULT_ENCODING

Native = Union[TextFormat, DefaultDisplay]
DisplayObject = Union[str, Markdown, HTML]
Paragraph = Union[str, Iterable, None]
Style = Union[str, Auto]

H_STYLE = None
P_STYLE = 'line-height: 1.1em; margin-top: 0em; margin-bottom: 0em; padding-top: 0em; padding-bottom: 0em;'
SPACE = ' '
HTML_SPACE = '&nbsp;'


class DocumentFormat(TextFormat, DefaultDisplay):
    def __init__(
            self,
            ending: str = DEFAULT_ENDING,
            encoding: str = DEFAULT_ENCODING,
            compress: Compress = None,
    ):
        self._current_paragraph = list()
        super().__init__(ending=ending, encoding=encoding, compress=compress)

    def get_current_paragraph(self) -> list:
        return self._current_paragraph

    def set_current_paragraph(self, paragraph: list) -> Native:
        self._current_paragraph = paragraph
        return self

    def clear_current_paragraph(self) -> Native:
        return self.set_current_paragraph(list())

    def get_encoded_paragraph(
            self,
            paragraph: Paragraph = None,
            level: Count = None,
            style: Style = AUTO,
            clear: bool = True,  # deprecated
    ) -> Iterator[str]:
        yield from self.get_current_paragraph()
        if clear:
            self.clear_current_paragraph()
        yield from super().get_encoded_paragraph(paragraph)

    @staticmethod
    def _get_display_class() -> Class:
        return str

    @classmethod
    def _get_display_object(cls, data: Union[str, Iterable, None]) -> Optional[DisplayObject]:
        if not data:
            return None
        if not isinstance(data, str):
            data = '\n'.join(data)
        display_class = cls._get_display_class()
        if display_class:
            return display_class(data)
        else:
            return str(data)

    @deprecated_with_alternative('append()')
    def append_to_current_paragraph(self, line: str) -> Native:
        self._current_paragraph.append(line)
        return self

    def append(self, line: str) -> None:
        if line:
            self._current_paragraph.append(line)
        else:
            return self.display_paragraph()

    @deprecated_with_alternative('append()')
    def output_line(self, line: str, output: AutoOutput = AUTO) -> None:
        if line:
            return self.append(line)
        else:
            return self.display_paragraph()

    @deprecated_with_alternative('_get_display_class|object|method()')
    def get_output(self, output: AutoOutput = AUTO):
        if Auto.is_auto(output):
            return self._get_display_method()
        else:
            return super().get_output(output=output)

    @staticmethod
    def _get_display_method() -> Callable:
        return display

    def display_paragraph(self, paragraph: Optional[Iterable] = None, level: Count = None, style: Style = AUTO):
        if level and paragraph:
            self.display_paragraph(None)
        data = self.get_encoded_paragraph(paragraph, level=level, style=style, clear=True)
        data = list(data)
        if data:
            obj = self._get_display_object(data)
            return display(obj)

    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            style: Union[str, Auto, None] = AUTO,
            output: AutoOutput = AUTO,
    ):
        self.display_paragraph()
        data = self.get_encoded_sheet(records, columns=columns, count=count, with_title=with_title, style=style)
        sheet = self._get_display_object(data)
        return display(sheet)


class MarkdownFormat(DocumentFormat):
    @staticmethod
    def _get_display_class():
        return Markdown

    def get_encoded_paragraph(
            self,
            paragraph: Paragraph = None,
            level: Count = None,
            clear: bool = True,  # deprecated
            style: Style = AUTO,
    ) -> Iterator[str]:
        paragraph = super().get_encoded_paragraph(paragraph, clear=clear)
        yield from self.get_md_text_code(paragraph, level=level)

    @staticmethod
    def get_md_text_code(lines: Iterable[str], level: Optional[int] = None) -> Iterator[str]:
        text = '\n'.join(lines)
        if level:
            prefix = '#' * level
            yield f'{prefix} {text}'
        else:
            yield text


class HtmlFormat(DocumentFormat):
    @staticmethod
    def _get_display_class():
        return HTML

    def get_encoded_paragraph(
            self,
            paragraph: Paragraph = None,
            level: Optional[int] = None,
            clear: bool = True,  # deprecated
            style: Union[str, Auto] = AUTO,
    ) -> Iterator[str]:
        paragraph = super().get_encoded_paragraph(paragraph, clear=clear)
        for html_string in self.get_html_text_code(paragraph, level=level, style=style):
            yield html_string.replace(SPACE * 2, HTML_SPACE * 2)

    def get_encoded_sheet(
            self,
            records: Iterable,
            columns: Iterable,
            count: AutoCount,
            with_title: bool,
            style: Style = AUTO,
    ) -> Iterator[str]:
        columns = list(self._get_column_names(columns))
        html_code_lines = self.get_html_table_code(records, columns, count, with_title, style=style)
        return map(lambda i: i.replace(SPACE * 2, HTML_SPACE * 2), html_code_lines)

    @staticmethod
    def get_html_text_code(
            lines: Iterable[str],
            level: Optional[int] = None,
            style: Union[str, Auto] = AUTO,
    ) -> Iterator[str]:
        if isinstance(lines, str):
            lines = lines.split('\n')
        assert isinstance(lines, Iterable), f'got {lines}'
        text = '<br>\n'.join(lines)
        if level:
            tag = f'h{level}'
            style = Auto.acquire(style, H_STYLE)
        else:
            tag = 'p'
            style = Auto.acquire(style, P_STYLE)
        open_tag = f'<{tag} style="{style}">' if style else f'<{tag}>'
        close_tag = f'</{tag}>'
        if text:
            yield f'{open_tag}{text}{close_tag}'

    @staticmethod
    def get_html_table_code(
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            style: str = None,
    ) -> Generator:
        yield '<table>'
        if with_title:
            yield '<thead>'
            yield '<tr>'
            for c in columns:
                yield f'<th>{c}</th>'
            yield '</tr>'
        yield '</thead>'
        yield '<tbody>'
        for n, r in list(enumerate(records)):
            if Auto.is_defined(count):
                if n >= count:
                    break
            yield '<tr>'
            for col in columns:
                value = r.get(col)
                if Auto.is_defined(style):
                    yield f'<td>{value}</td>'
                else:
                    yield f'<td style="{style}">{value}</td>'
            yield '</tr>'
        yield '</tbody>'
        yield '</table>'


if HTML:
    DisplayMixin.set_display(HtmlFormat())
