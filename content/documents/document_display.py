from typing import Optional, Callable, Iterable, Sequence, Union

try:  # Assume we're a submodule in a package.
    from interfaces import Item, ItemType, ContentType, Class, Count, AutoCount, Auto, AUTO
    from base.constants.chars import SPACE, HTML_SPACE
    from base.classes.display import DefaultDisplay, PREFIX_FIELD
    from base.classes.enum import ClassType
    from base.mixin.display_mixin import DisplayMixin, AutoOutput, Class
    from base.mixin.iter_data_mixin import IterDataMixin
    from utils.external import display, clear_output, HTML, Markdown
    from content.documents.display_mode import DisplayMode
    from content.documents.document_item import DocumentItem, Paragraph, Sheet, Chart, Chapter
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Item, ItemType, ContentType, Class, Count, AutoCount, Auto, AUTO
    from ...base.constants.chars import SPACE, HTML_SPACE
    from ...base.classes.display import DefaultDisplay, PREFIX_FIELD
    from ...base.classes.enum import ClassType
    from ...base.mixin.display_mixin import DisplayMixin, AutoOutput, Class
    from ...base.mixin.iter_data_mixin import IterDataMixin
    from ...utils.external import display, clear_output, HTML, Markdown
    from .display_mode import DisplayMode
    from .document_item import DocumentItem, Paragraph, Sheet, Chart, Chapter

Native = Union[DefaultDisplay, IterDataMixin]
Style = Union[str, Auto]
FormattedDisplayTypes = Union[Markdown, HTML]
DisplayObject = Union[FormattedDisplayTypes, str]
FORMATTED_DISPLAY_TYPES = Markdown, HTML

H_STYLE = None
P_STYLE = 'line-height: 1.1em; margin-top: 0em; margin-bottom: 0em; padding-top: 0em; padding-bottom: 0em;'


class DocumentDisplay(DefaultDisplay, IterDataMixin):
    display_mode: DisplayMode = DisplayMode.Text

    def __init__(
            self,
            accumulated_document: Chapter = Chapter([]),
            current_paragraph: Optional[list] = None,
    ):
        self.accumulated_document = accumulated_document
        self._current_paragraph = current_paragraph or list()
        super().__init__()

    def get_current_paragraph(self) -> list:
        return self._current_paragraph

    def set_current_paragraph(self, paragraph: list) -> Native:
        self._current_paragraph = paragraph
        return self

    def clear_current_paragraph(self) -> Native:
        return self.set_current_paragraph(list())

    def add_to_paragraph(self, text: str, wait: bool = True) -> Native:
        lines = text.split('\n')
        if self.accumulated_document.has_data():
            last_item = self.accumulated_document.get_data()[-1]
        else:
            last_item = None
        if isinstance(last_item, Paragraph):
            last_item.add(lines)
        else:
            self.accumulated_document.add(Paragraph(lines))
        return self

    @classmethod
    def _get_display_class(cls) -> Class:
        return cls.display_mode.get_class()

    @classmethod
    def _get_display_object(cls, data: Union[DocumentItem, str, Iterable, None]) -> Optional[DisplayObject]:
        if not data:
            return None
        elif isinstance(data, DocumentItem):
            if cls.display_mode == DisplayMode.Text:
                code = data.get_text()
            elif cls.display_mode == DisplayMode.Md:
                code = data.get_md_code()
            elif cls.display_mode == DisplayMode.Html:
                code = data.get_html_code()
            else:
                code = data
        elif isinstance(data, str):
            code = data
        elif isinstance(data, Iterable):
            code = '\n'.join(data)
        else:
            raise TypeError
        display_class = cls._get_display_class()
        if display_class:
            return display_class(code)
        else:
            return str(code)

    def display(self, item: Union[DocumentItem, FormattedDisplayTypes, Auto] = AUTO):
        method = self._get_display_method()
        if isinstance(item, FORMATTED_DISPLAY_TYPES):
            return method(item)
        else:
            obj = self._get_display_object(item)
            return method(obj)

    def display_all(self, refresh: bool = False) -> Native:
        if refresh:
            self.clear_output(wait=True)
        self.display(self.accumulated_document)
        return self

    def append(self, item: Union[DocumentItem, str], show: bool = True) -> None:
        if isinstance(item, str):
            self.add_to_paragraph(item)
        elif isinstance(item, DocumentItem):
            self.accumulated_document.add(item)
        else:
            raise TypeError(f'Expected DocumentItem, got {item}')
        if show:
            self.display(item)

    @staticmethod
    def _get_display_method() -> Callable:
        return display

    def display_paragraph(
            self,
            paragraph: Optional[Paragraph, Iterable] = None,
            level: Count = None,
            style: Style = AUTO,
            name: str = '',
    ):
        if level and paragraph:
            self.display_paragraph(None)
        data = self.get_encoded_paragraph(paragraph, level=level, style=style, clear=True)
        paragraph = Paragraph(list(data), level=level, style=style, name=name)
        if paragraph:
            obj = self._get_display_object(paragraph)
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

    def clear_output(self, wait: bool = False) -> Native:
        # self.clear_current_paragraph()
        self.display_paragraph()
        clear_output(wait=wait)
        return self

    def refresh(self) -> Native:
        self.clear_output(wait=True)
        self.display_all(refresh=False)
        return self


if HTML:
    DocumentDisplay.display_mode = DisplayMode.Html
DisplayMixin.set_display(DocumentDisplay())
