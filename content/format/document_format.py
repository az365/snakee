from typing import Optional, Iterable, Generator, Sequence

try:  # Assume we're a submodule in a package.
    from interfaces import Item, ItemType, ContentType, AutoCount, Auto, AUTO
    from base.mixin.line_output_mixin import LineOutputMixin, AutoOutput
    from utils.external import display, HTML, Markdown
    from content.format.text_format import TextFormat
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Item, ItemType, ContentType, AutoCount, Auto, AUTO
    from ...base.mixin.line_output_mixin import LineOutputMixin, AutoOutput
    from ...utils.external import display, HTML, Markdown
    from .text_format import TextFormat


class DocumentFormat(TextFormat, LineOutputMixin):
    def output_line(self, line: str, output: AutoOutput = AUTO) -> None:
        if Auto.is_auto(output):
            display(line)
        else:
            return super().output_line(line, output=output)

    def output_paragraph(self, paragraph, level: Optional[int] = None, output: AutoOutput = AUTO):
        self.output_line(str(paragraph), output=output)


class MarkdownFormat(DocumentFormat):
    def output_paragraph(self, paragraph, level: Optional[int] = None, output: AutoOutput = AUTO) -> None:
        md_code = self.get_md_text_code(paragraph, level=level)
        if Markdown:
            md_obj = Markdown(md_code)
        else:
            md_obj = str(md_code)
        return self.output_line(md_obj, output=output)

    @staticmethod
    def get_md_text_code(text: str, level: Optional[int] = None) -> str:
        if level:
            prefix = '#' * level
            return f'{prefix} {text}'
        else:
            return text


class HtmlFormat(DocumentFormat):
    def output_paragraph(self, paragraph, level: Optional[int] = None, output: AutoOutput = AUTO) -> None:
        html_code = self.get_html_text_code(paragraph, level=level)
        if HTML:
            html_obj = HTML(html_code)
        else:
            html_obj = str(html_code)
        return self.output_line(html_obj, output=output)

    def output_table(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            output: AutoOutput = AUTO,
    ) -> None:
        html_code = '\n'.join(self.get_html_table_code(records, columns, count, with_title))
        if HTML:
            html_obj = HTML(html_code)
        else:
            html_obj = str(html_code)
        return self.output_line(html_obj, output=output)

    @staticmethod
    def get_html_text_code(text: str, level: Optional[int] = None, style=None) -> str:
        if level:
            tag = f'h{level}'
        else:
            tag = 'p'
        open_tag = f'<{tag} style="{style}">' if style else f'<{tag}>'
        close_tag = f'</{tag}>'
        return f'{open_tag}{text}{close_tag}'

    @staticmethod
    def get_html_table_code(
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
    ) -> Generator:
        yield '<table>'
        if with_title:
            for c in columns:
                yield f'<th>{c}</th>'
        for n, r in enumerate(records):
            if count:
                if n >= count:
                    break
            yield '<tr>'
            for c in columns:
                yield f'<td>{c}</td>'
            yield '</tr>'
        yield '</table>'
