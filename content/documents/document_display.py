from typing import Optional, Callable, Iterable, Sequence, Union

try:  # Assume we're a submodule in a package.
    from interfaces import Item, ItemType, ContentType, Class, Count, Auto
    from base.constants.chars import EMPTY, SPACE, HTML_SPACE, PARAGRAPH_CHAR
    from base.classes.display import DefaultDisplay, DEFAULT_CHAPTER_TITLE_LEVEL
    from base.classes.enum import ClassType
    from base.functions.arguments import get_name
    from base.mixin.display_mixin import DisplayMixin, Class
    from base.mixin.iter_data_mixin import IterDataMixin
    from utils.external import display, clear_output, HTML, Markdown
    from content.documents.display_mode import DisplayMode
    from content.documents.document_item import DocumentItem, Paragraph, Sheet, Chart, Chapter
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Item, ItemType, ContentType, Class, Count, Auto
    from ...base.constants.chars import EMPTY, SPACE, HTML_SPACE, PARAGRAPH_CHAR
    from ...base.classes.display import DefaultDisplay, DEFAULT_CHAPTER_TITLE_LEVEL
    from ...base.classes.enum import ClassType
    from ...base.functions.arguments import get_name
    from ...base.mixin.display_mixin import DisplayMixin, Class
    from ...base.mixin.iter_data_mixin import IterDataMixin
    from ...utils.external import display, clear_output, HTML, Markdown
    from .display_mode import DisplayMode
    from .document_item import DocumentItem, Paragraph, Sheet, Chart, Chapter

Native = Union[DefaultDisplay, IterDataMixin]
Style = Optional[str]
FormattedDisplayTypes = Union[Markdown, HTML]
DisplayedItem = Union[DocumentItem, FormattedDisplayTypes, None]
DisplayedData = Union[DocumentItem, str, Iterable, None]
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

    def get_data(self):
        return self._accumulated_document

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
    def get_header_chapter_for(cls, obj, level: int = 1, comment: str = EMPTY, name: Optional[str] = None) -> Chapter:
        obj_name = get_name(obj)
        if hasattr(obj, 'get_str_title'):
            title = obj.get_str_title()
        else:
            title = obj_name
        if not Auto.is_defined(name):
            name = f'{obj_name} header'
        chapter = Chapter(name=name)
        if level:
            chapter.append(Paragraph(title, level=level), inplace=True)
        str_headers = None
        if hasattr(obj, 'get_str_headers'):
            str_headers = list(obj.get_str_headers())
        if hasattr(obj, 'get_caption') and not str_headers:
            str_headers = [obj.get_caption()]
        if comment:
            str_headers.append(comment)
        if str_headers:
            chapter.append(Paragraph(str_headers), inplace=True)
        return chapter

    @classmethod
    def get_meta_chapter_for(
            cls,
            obj,
            level: Optional[int] = DEFAULT_CHAPTER_TITLE_LEVEL,
            name: str = 'Meta',
    ) -> Chapter:
        chapter = Chapter(name=name)
        if level:
            title = Paragraph([name], level=level, name=f'{name} title')
            chapter.add(title, inplace=True)
        meta_sheet = cls.get_meta_sheet_for(obj, name=f'{name} sheet')
        chapter.add_items([meta_sheet], inplace=True)
        return chapter

    @staticmethod
    def _is_formatted_item(item: DisplayedItem) -> bool:
        is_formatted_types_imported = min(map(bool, FORMATTED_DISPLAY_TYPES))
        if is_formatted_types_imported:
            return isinstance(item, FORMATTED_DISPLAY_TYPES)

    @classmethod
    def _get_display_code_from_document_item(cls, data: DocumentItem) -> str:
        if cls.display_mode == DisplayMode.Text:
            return data.get_text()
        elif cls.display_mode == DisplayMode.Md:
            return data.get_md_code()
        elif cls.display_mode == DisplayMode.Html:
            return data.get_html_code()
        else:
            return str(data)

    @classmethod
    def _get_display_code_from_document_iterable(cls, data: Iterable) -> str:
        lines = list()
        for i in data:
            if isinstance(i, DocumentItem):
                lines.append(cls._get_display_code_from_document_item(i))
            elif isinstance(i, str):
                lines.append(i)
            else:
                lines.append(str(i))
        return PARAGRAPH_CHAR.join(lines)

    @classmethod
    def _get_display_code(cls, data: DisplayedData) -> Optional[str]:
        if not data:
            return None
        elif isinstance(data, DocumentItem) or hasattr(data, 'get_text'):  # Text, Paragraph, Sheet, Chart, Container...
            return cls._get_display_code_from_document_item(data)
        elif isinstance(data, str):
            return data
        elif isinstance(data, Iterable):
            return cls._get_display_code_from_document_iterable(data)
        else:
            raise TypeError(f'Expected data as DocumentItem, Iterable or str, got {data}')

    @classmethod
    def _get_display_class(cls) -> Class:
        return cls.display_mode.get_class()

    @classmethod
    def _get_display_object(cls, data: DisplayedData) -> Optional[DisplayObject]:
        code = cls._get_display_code(data)
        if code:
            display_class = cls._get_display_class()
            if display_class:
                return display_class(code)
            else:
                return str(code)

    def display(
            self,
            item: DisplayedItem = None,
            save: bool = False,
            refresh: Optional[bool] = None,
    ):
        if save:
            self.append(item, show=False)
        if not Auto.is_defined(refresh):
            refresh = self.is_partially_shown()
        if refresh:
            return self.display_all(refresh=True)
        else:
            method = self._get_display_method()
            if self._is_formatted_item(item):
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

    def append(self, item: Union[DocumentItem, str], show: bool = True, refresh: Optional[bool] = None) -> Native:
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

    @staticmethod
    def build_paragraph(data: Iterable, level: Count = 0, name: str = EMPTY) -> Paragraph:
        if isinstance(data, str):
            data = [data]
        return Paragraph(data, level=level, name=name)

    # @deprecated
    def display_paragraph(
            self,
            paragraph: Union[Paragraph, Iterable, str, None] = None,
            level: Count = None,
            style: Style = None,
            name: str = EMPTY,
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

    # @deprecated
    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: Count = None,
            item_type: ItemType = ItemType.Record,
            with_title: bool = True,
            style: Style = None,
    ):
        self.display_current_paragraph(save=True, clear=True)
        sheet = Sheet.from_records(records, columns=columns)
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
DocumentDisplay.set_sheet_class_inplace(Sheet)
DisplayMixin.set_display(DocumentDisplay())
DefaultDisplay.display = DocumentDisplay()
