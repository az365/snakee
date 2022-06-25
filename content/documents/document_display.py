from typing import Optional, Callable, Iterable, Sequence, Union

try:  # Assume we're a submodule in a package.
    from interfaces import Item, ItemType, ContentType, Class, Count, AutoCount, AutoBool, Auto, AUTO
    from base.constants.chars import EMPTY, SPACE, HTML_SPACE, PARAGRAPH_CHAR
    from base.classes.display import DefaultDisplay, PREFIX_FIELD
    from base.classes.enum import ClassType
    from base.mixin.display_mixin import DisplayMixin, AutoOutput, Class
    from base.mixin.iter_data_mixin import IterDataMixin
    from utils.external import display, clear_output, HTML, Markdown
    from streams.stream_type import StreamType
    from content.documents.display_mode import DisplayMode
    from content.documents.document_item import DocumentItem, Paragraph, Sheet, Chart, Chapter
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Item, ItemType, ContentType, Class, Count, AutoCount, AutoBool, Auto, AUTO
    from ...base.constants.chars import EMPTY, SPACE, HTML_SPACE, PARAGRAPH_CHAR
    from ...base.classes.display import DefaultDisplay, PREFIX_FIELD
    from ...base.classes.enum import ClassType
    from ...base.mixin.display_mixin import DisplayMixin, AutoOutput, Class
    from ...base.mixin.iter_data_mixin import IterDataMixin
    from ...utils.external import display, clear_output, HTML, Markdown
    from ...streams.stream_type import StreamType
    from .display_mode import DisplayMode
    from .document_item import DocumentItem, Paragraph, Sheet, Chart, Chapter

Native = Union[DefaultDisplay, IterDataMixin]
Style = Union[str, Auto]
FormattedDisplayTypes = Union[Markdown, HTML]
DisplayObject = Union[FormattedDisplayTypes, str]
FORMATTED_DISPLAY_TYPES = Markdown, HTML


class DocumentDisplay(DefaultDisplay, IterDataMixin):
    display_mode: DisplayMode = DisplayMode.Text

    def __init__(
            self,
            accumulated_document: Chapter = Chapter([]),
            accumulated_lines: Optional[list] = None,
    ):
        self._accumulated_document = accumulated_document
        self._accumulated_lines = accumulated_lines or list()
        self._is_partially_shown = False
        super().__init__()

    def get_accumulated_document(self) -> Chapter:
        return self._accumulated_document

    def get_accumulated_lines(self) -> list:
        return self._accumulated_lines

    def set_accumulated_lines(self, lines: Iterable) -> Native:
        if not isinstance(lines, list):
            lines = list(lines)
        self._accumulated_lines = lines
        return self

    def get_current_paragraph(self) -> Paragraph:
        return Paragraph(self.get_accumulated_lines())

    def set_current_paragraph(self, paragraph: Paragraph) -> Native:
        return self.set_accumulated_lines(paragraph.get_lines())

    def clear_current_paragraph(self) -> Native:
        return self.set_accumulated_lines(list())

    def is_partially_shown(self) -> bool:
        return self._is_partially_shown

    def set_partially_shown(self, shown: bool) -> Native:
        self._is_partially_shown = shown
        return self

    def add_to_paragraph(self, text: Union[str, Iterable], wait: bool = True) -> Native:
        if isinstance(text, str):
            lines = text.split(PARAGRAPH_CHAR)
        elif isinstance(text, Iterable):
            lines = text
        elif text is None:
            lines = [EMPTY]
        else:
            raise TypeError(f'Expected text as str or Iterable, got {text}')
        for line in lines:
            if line:
                self.get_accumulated_lines().append(line)
            else:
                self.display_current_paragraph()
        if not wait:
            self.display_current_paragraph(save=False, clear=False)
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
            code = PARAGRAPH_CHAR.join(data)
        else:
            raise TypeError
        display_class = cls._get_display_class()
        if display_class:
            return display_class(code)
        else:
            return str(code)

    def display(
            self,
            item: Union[DocumentItem, FormattedDisplayTypes, Auto] = AUTO,
            save: bool = False,
            refresh: AutoBool = AUTO,
    ):
        if save:
            self.append(item, show=False)
        refresh = Auto.delayed_acquire(refresh, self.is_partially_shown)
        if refresh:
            return self.display_all(refresh=True)
        else:
            method = self._get_display_method()
            if isinstance(item, FORMATTED_DISPLAY_TYPES):
                return method(item)
            else:
                obj = self._get_display_object(item)
                if obj:
                    return method(obj)

    def display_current_paragraph(self, save: bool = True, clear: bool = True, refresh: bool = False) -> Native:
        if refresh:
            self.clear_output(wait=True)
        item = self.get_current_paragraph()
        if item:
            self.display(item, save=save)
        if clear:
            self.clear_current_paragraph()
            self.set_partially_shown(False)
        else:
            self.set_partially_shown(True)
        return self

    def display_all(self, refresh: bool = False) -> Native:
        if refresh:
            self.clear_output(wait=True)
        self.display(self.get_accumulated_document(), save=False, refresh=False)
        return self

    def append(self, item: Union[DocumentItem, str], show: bool = True, refresh: AutoBool = AUTO) -> Native:
        if item:
            if isinstance(item, str):
                self.add_to_paragraph(item)
            elif isinstance(item, DocumentItem):
                self.get_accumulated_document().add(item, inplace=True)
            elif isinstance(item, Iterable):
                for i in item:
                    self.append(i)
            else:
                raise TypeError(f'Expected DocumentItem, got {item}')
            if show:
                self.display(item, save=False, refresh=refresh)
        return self

    @staticmethod
    def _get_display_method() -> Callable:
        return display

    def display_paragraph(
            self,
            paragraph: Union[Paragraph, Iterable, str, None] = None,
            level: Count = None,
            style: Style = AUTO,
            name: str = '',
    ):
        if level and paragraph:
            self.display_current_paragraph(save=True, clear=True, refresh=False)
        if isinstance(paragraph, Paragraph):
            paragraph.add(self.get_accumulated_lines(), before=True, inplace=True)
        else:
            self.add_to_paragraph(paragraph)
            paragraph = self.get_current_paragraph()
        if level:
            paragraph.set_level(level, inplace=True)
        response = self.display(paragraph, save=True)
        self.clear_current_paragraph()
        return response

    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            style: Union[str, Auto, None] = AUTO,
            output: AutoOutput = AUTO,
    ):
        self.display_current_paragraph(save=True, clear=True)
        column_names = self._extract_column_names(columns)
        stream = StreamType.RecordStream.stream(records, struct=column_names)
        sheet = Sheet(stream)
        return self.display(sheet)

    @staticmethod
    def _extract_column_names(columns: Iterable) -> list:
        column_names = list()
        for c in columns:
            if isinstance(c, (str, int)):
                column_names.append(c)
            elif isinstance(c, (list, tuple)):
                column_names.append(c[0])
            else:
                raise TypeError(f'Expected column as Name or Sequence, got {c}')
        return column_names

    def clear_output(self, wait: bool = False, save_paragraph: bool = True) -> Native:
        if save_paragraph:
            self.display_current_paragraph(save=True, clear=True, refresh=False)
        clear_output(wait=wait)
        return self

    def refresh(self) -> Native:
        self.clear_output(wait=True)
        self.display_all(refresh=False)
        return self


if HTML:
    DocumentDisplay.display_mode = DisplayMode.Html
DisplayMixin.set_display(DocumentDisplay())
