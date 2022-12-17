from typing import Optional, Callable, Iterable, Iterator, Sequence, Tuple, Union, Any

try:  # Assume we're a submodule in a package.
    from base.constants.chars import EMPTY, SPACE, HTML_SPACE, HTML_INDENT, PARAGRAPH_CHAR, REPR_DELIMITER
    from base.interfaces.sheet_interface import SheetInterface, Record, Row, FormattedRow, Columns, Count
    from base.classes.simple_sheet import SimpleSheet, SheetMixin, SheetItems, get_name, DEFAULT_LINE_LEN
    from base.classes.enum import DynamicEnum
    from base.constants.chars import CROP_SUFFIX
    from base.classes.typing import AUTO, Auto, Name
    from base.abstract.simple_data import SimpleDataWrapper, SimpleDataInterface
    from base.mixin.iter_data_mixin import IterDataMixin
    from base.mixin.map_data_mixin import MapDataMixin
    from functions.primary.items import get_fields_values_from_item, get_field_value_from_item
    from utils.external import Markdown, HTML, display
    from content.documents.display_mode import DisplayMode
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import EMPTY, SPACE, HTML_SPACE, HTML_INDENT, PARAGRAPH_CHAR, REPR_DELIMITER
    from ...base.interfaces.sheet_interface import SheetInterface, Record, Row, FormattedRow, Columns, Count
    from ...base.classes.simple_sheet import SimpleSheet, SheetMixin, SheetItems, get_name, DEFAULT_LINE_LEN
    from ...base.classes.enum import DynamicEnum
    from ...base.constants.chars import CROP_SUFFIX
    from ...base.classes.typing import AUTO, Auto, Name
    from ...base.abstract.simple_data import SimpleDataWrapper, SimpleDataInterface
    from ...base.mixin.iter_data_mixin import IterDataMixin
    from ...base.mixin.map_data_mixin import MapDataMixin
    from ...functions.primary.items import get_fields_values_from_item, get_field_value_from_item
    from ...utils.external import Markdown, HTML, display
    from .display_mode import DisplayMode

HtmlStyle = str
ContentStyle = Any
Style = Union[HtmlStyle, ContentStyle]
OptStyle = Optional[Style]
DisplayObject = Union[str, Markdown, HTML]

H_STYLE = None
P_STYLE = 'line-height: 1.1em; margin-top: 0em; margin-bottom: 0em; padding-top: 0em; padding-bottom: 0em;'
DEFAULT_CHAPTER_TITLE_LEVEL = 3
SHORT_LINE_LEN = 20


class DocumentItem(SimpleDataWrapper):
    def __init__(self, data, style: OptStyle = None, name: str = ''):
        self._style = style
        super().__init__(data=data, name=name)

    def get_items(self) -> Iterator:
        data = self.get_data()
        if isinstance(data, str):
            yield data
        elif hasattr(data, 'get_items'):  # isinstance(data, RegularStream)
            yield from data.get_items()
        elif isinstance(data, Iterable):
            yield from data
        elif data:
            raise TypeError(f'Expected str, Iterable or Stream, got {data}')

    def get_lines(self) -> Iterator[str]:
        data = self.get_data()
        if isinstance(data, str):
            yield data
        elif hasattr(data, 'get_lines'):  # isinstance(data, RegularStream)
            yield from data.get_lines()
        elif isinstance(data, Iterable):
            for i in data:
                if isinstance(i, str):
                    yield i
                elif isinstance(i, DocumentItem) or hasattr(i, 'get_lines'):
                    yield from i.get_lines()
                elif hasattr(i, 'get_text'):
                    yield i.get_text()
                else:
                    yield str(i)
        elif data:
            raise TypeError(f'Expected str, Iterable or Stream, got {data}')

    def get_text(self) -> str:
        data = self.get_data()
        if isinstance(data, str):
            return data
        elif isinstance(data, Iterable):
            return PARAGRAPH_CHAR.join(map(str, data))

    def get_style(self) -> OptStyle:
        return self._style

    def get_html_style(self) -> Optional[HtmlStyle]:
        style = self.get_style()
        if isinstance(style, HtmlStyle):
            return style
        elif hasattr(style, 'get_html_style'):  # isinstance(style, ContentStyle)
            return style.get_html_style()

    def get_html_attributes(self) -> Iterator[Tuple[str, Any]]:
        style = self.get_html_style()
        if Auto.is_defined(style):
            if style:
                yield 'style', str(style)
        name = self.get_name()
        if Auto.is_defined(name):
            if name:
                yield 'name', name

    def get_html_attr_str(self) -> str:
        return SPACE.join([f'{k}={repr(v)}' for k, v in self.get_html_attributes()])

    def has_html_tags(self) -> bool:
        for _ in self.get_html_attributes():
            return True
        return False

    def get_html_tag_name(self) -> str:
        return 'div'

    def get_html_open_tag(self) -> str:
        tag = self.get_html_tag_name()
        attributes = self.get_html_attr_str()
        if attributes:
            return f'<{tag} {attributes}>'
        elif tag:
            return f'<{tag}>'
        else:
            return EMPTY

    def get_html_close_tag(self) -> str:
        tag = self.get_html_tag_name()
        attributes = self.get_html_attr_str()
        if attributes or tag:
            return f'</{tag}>'
        else:
            return EMPTY

    def get_items_html_lines(self) -> Iterator[str]:
        for item in self.get_items():
            if isinstance(item, str):
                yield item
            elif isinstance(item, DocumentItem) or hasattr(item, 'get_items_html_lines'):
                yield from item.get_items_html_lines()
            elif isinstance(item, DocumentItem) or hasattr(item, 'get_html_lines'):  # ?
                yield from item.get_html_lines()
            elif hasattr(item, 'get_lines'):  # isinstance(item, RegularStream)
                yield from item.get_lines()
            elif isinstance(item, Iterable):
                yield from item
            else:
                raise TypeError(f'Expected item as DocumentItem or str, got item {item} as {type(item)}')

    def get_html_lines(self) -> Iterator[str]:
        if self.has_html_tags():
            yield self.get_html_open_tag()
        yield from self.get_items_html_lines()
        if self.has_html_tags():
            yield self.get_html_close_tag()

    def get_md_lines(self) -> Iterator[str]:
        yield from self.get_lines()

    def get_md_code(self) -> str:
        return PARAGRAPH_CHAR.join(self.get_md_lines())

    def get_html_code(self) -> str:
        return PARAGRAPH_CHAR.join(self.get_html_lines())

    def get_html_object(self) -> HTML:
        return HTML(self.get_html_code())

    def get_md_object(self) -> Markdown:
        return Markdown(self.get_md_code())

    def get_object(self, display_mode: DisplayMode) -> DisplayObject:
        if display_mode == DisplayMode.Md:
            return self.get_md_object()
        elif display_mode == DisplayMode.Html:
            return self.get_html_object()
        else:
            return self.get_text()

    @staticmethod
    def _get_display_method(method: Union[Callable, Auto, None] = AUTO) -> Callable:
        if Auto.is_defined(method):
            return method
        else:
            return display

    def show(self, display_mode: DisplayMode = DisplayMode.Html):
        method = self._get_display_method()
        obj = self.get_object(display_mode=display_mode)
        if method:
            return method(obj)
        else:
            return obj


Native = Union[DocumentItem, IterDataMixin]
Items = Iterable[DocumentItem]


class Sheet(DocumentItem, IterDataMixin, SheetMixin, SheetInterface):
    def __init__(self, data: SheetItems, columns: Columns = None, name: str = ''):
        self._struct = None
        super().__init__(data=list(), name=name)
        self._set_struct_inplace(columns)
        self._set_items_inplace(data)

    def _set_items_inplace(self, items: SheetItems) -> None:
        expected_columns = self.get_columns()
        if hasattr(items, 'get_list'):  # isinstance(items, RegularStreamInterface)
            self._set_data_inplace(items)
            if hasattr(items, 'get_struct') and not expected_columns:  # isinstance(items, Stream):
                detected_struct = items.get_struct()
                if not detected_struct:
                    detected_struct = items.get_columns()
                if detected_struct:
                    self._set_struct_inplace(detected_struct)
            super()._set_items_inplace(items)
        elif isinstance(items, Iterable) and not isinstance(items, str):
            super()._set_items_inplace(items)
        else:
            raise TypeError(f'Expected items as RegularStream, got {items} as {type(items)}')

    @classmethod
    def from_records(cls, records: Iterable[Record], columns: Columns = None, name: Name = EMPTY) -> Native:
        if Auto.is_defined(columns):
            column_names = cls._get_column_names_from_columns(columns)
        else:
            records = list(records)
            column_names = cls._get_column_names_from_records(records)
            columns = column_names
        rows = list()
        for record in records:
            row = [record.get(c) for c in column_names]
            rows.append(tuple(row))
        return Sheet(data=rows, columns=columns, name=name)

    def get_data(self) -> SheetItems:
        data = super().get_data()
        return data

    def get_struct(self) -> Sequence:
        return self._struct

    def _set_struct_inplace(self, struct: Sequence):
        self._struct = struct

    def get_columns(self) -> list:
        struct = self.get_struct()
        if hasattr(struct, 'get_columns'):  # isinstance(struct, StructInterface)
            columns = struct.get_columns()
        elif isinstance(struct, Iterable):
            columns = self._get_column_names_from_columns(struct)
        else:
            columns = self._get_column_names_from_items(self.get_data())
        return list(columns)

    def get_column_names(self) -> list:
        column_names = list()
        for no, field in enumerate(self.get_struct() or []):
            if isinstance(field, (list, tuple)) and not isinstance(field, str):
                if field:
                    name = field[0]
                else:
                    name = no
            else:
                name = get_name(field)
            column_names.append(name)
        return column_names

    def get_column_lens(self, default: Optional[int] = None) -> list:
        column_lens = list()
        struct = self.get_struct()
        if struct:
            for field in struct:
                length = None
                if isinstance(field, (list, tuple)) and not isinstance(field, str):
                    if len(field) > 1:
                        length = field[-1]
                elif hasattr(field, 'get_representation'):  # isinstance(field, AnyField):
                    length = field.get_representation().get_max_value_len()
                if length is None:
                    length = default
                column_lens.append(length)
        else:
            count = len(self.get_columns())
            column_lens = [default] * count
        return column_lens

    def get_records(self) -> Iterable:
        data = self.get_data()
        if hasattr(data, 'get_records'):  # isinstance(data, RegularStreamInterface):
            return data.get_records()
        else:
            return super().get_records()

    def get_rows(self, with_title: bool = False, upper_title: bool = False) -> Iterator[Row]:
        if with_title:
            yield self.get_title_row(upper_title=upper_title)
        data = self.get_data()
        if hasattr(data, 'get_rows'):  # isinstance(data, RegularStreamInterface):
            yield from map(Row, data.get_rows())
        else:
            yield from super().get_rows(with_title=False)

    def get_formatted_rows(self, with_title: bool = True, max_len: Count = DEFAULT_LINE_LEN) -> Iterator[FormattedRow]:
        struct = self.get_struct()
        if hasattr(struct, 'get_field_representations'):  # isinstance(struct, FlatStruct):
            for row in self.get_rows(with_title=with_title):
                # yield struct.format(row)
                formatted_row = list()
                for cell, representation in zip(row, struct.get_field_representations()):
                    if representation and hasattr(representation, 'format'):  # isinstance(representation, RepresentationInterface):
                        formatted_cell = representation.format(cell)
                    else:
                        formatted_cell = self._crop_cell(cell, max_len)
                    formatted_row.append(formatted_cell)
                yield Row(formatted_row)
        else:
            yield from super().get_formatted_rows(with_title=with_title, max_len=max_len)

    def get_lines(self, delimiter: str = REPR_DELIMITER) -> Iterator[str]:
        data = self.get_data()
        if hasattr(data, 'get_lines'):  # isinstance(data, RegularStreamInterface)
            return data.get_lines()
        elif hasattr(data, 'get_items_of_type'):  # isinstance(data, RegularStreamInterface)
            return data.get_items_of_type('line')
        elif hasattr(data, 'get_items()'):
            return map(str, data.get_items())
        elif isinstance(data, Iterable):
            struct = self.get_struct()
            if hasattr(struct, 'format'):  # isinstance(struct, FlatStruct):
                for row in self.get_rows(with_title=True):
                    yield struct.format(row)
            else:
                placeholders = ['{:' + str(min_len) + '}' for min_len in self.get_column_lens()]
                formatter = delimiter.join(placeholders)
                for row in self.get_formatted_rows(with_title=True):
                    yield formatter.format(row)
        else:
            raise TypeError(f'Expected RegularStream, SimpleData or Iterable, got {data}')

    def has_html_tags(self) -> bool:
        return True

    def get_html_tag_name(self) -> str:
        return 'table'

    def get_html_open_tag(self) -> str:
        tag = self.get_html_tag_name()
        return f'<{tag}>'

    def get_title_html_lines(self) -> Iterator[str]:
        tag = 'thead'
        yield f'<{tag}>'
        yield HTML_INDENT + '<tr>'
        for c in self.get_columns():
            yield str(HTML_INDENT * 2) + f'<th>{c}</th>'
        yield HTML_INDENT + '</tr>'
        yield f'</{tag}>'

    def get_items_html_lines(self, count: Optional[int] = None) -> Iterator[str]:
        style = self.get_html_style()
        formatted_rows = self.get_formatted_rows(with_title=False)
        for n, row in enumerate(formatted_rows):
            yield '<tr>'
            for cell in row:
                if Auto.is_defined(style):
                    yield HTML_INDENT + f'<td style="{style}">{cell}</td>'
                else:
                    yield HTML_INDENT + f'<td>{cell}</td>'
            yield '</tr>'
            if Auto.is_defined(count):
                if n + 1 >= count:
                    break

    def get_html_lines(
            self,
            with_title: bool = True,
            count: Optional[int] = None,
    ) -> Iterator[str]:
        yield self.get_html_open_tag()
        if with_title:
            yield from self.get_title_html_lines()
        yield HTML_INDENT + '<tbody>'
        for line in self.get_items_html_lines(count=count):
            yield HTML_INDENT * 2 + line
        yield HTML_INDENT + '</tbody>'
        yield self.get_html_close_tag()

    def get_md_lines(self) -> str:
        return self.get_text()

    def get_count_repr(self, default: str = '<iter>') -> str:
        if not self.get_data().is_in_memory():
            self.set_data(self.get_data().collect(), inplace=True)
        count = self.get_data().get_count()
        return f'{count} items'


SimpleSheet.set_class(Sheet)


class Chart(DocumentItem):
    pass


class Text(DocumentItem, IterDataMixin):
    def __init__(
            self,
            data: Union[str, list, None] = None,
            style: OptStyle = None,
            name: str = '',
    ):
        self._style = style
        super().__init__(data=data, name=name)

    def add(
            self,
            text: Union[Native, Iterable],
            before: bool = False,
            inplace: bool = False,
            **kwargs
    ) -> Native:
        if text:
            assert not kwargs, f'Text.add() does not support kwargs, got {kwargs}'
            if self.is_empty():
                self.set_data(list(), inplace=True)
            if isinstance(text, str):
                lines = [text]
            elif isinstance(text, Iterable):
                lines = text
            else:
                raise TypeError(f'Expected text as str or Iterable, got {repr(text)}')
            return super().add(lines, before=before, inplace=inplace)
        else:
            return self

    def get_items_html_lines(self, add_br: bool = True, split_br: bool = True) -> Iterator[str]:
        for n, i in enumerate(super().get_items_html_lines()):
            if split_br:
                lines = i.split(PARAGRAPH_CHAR)
            else:
                lines = [i]
            for line in lines:
                if add_br:
                    if n and not line.startswith('<br>'):
                        yield f'<br>{line}'
                    else:
                        yield line
                else:
                    yield line

    def get_html_code(self) -> str:
        return EMPTY.join(self.get_html_lines())

    def get_cropped_text(self, max_len: int = SHORT_LINE_LEN) -> str:
        text = self.get_text()
        if Auto.is_defined(max_len):
            if len(text) > max_len:
                suffix_len = len(CROP_SUFFIX)
                crop_len = max_len - suffix_len
                if crop_len < 0:
                    crop_len = 0
                text = text[:crop_len] + CROP_SUFFIX
        return text

    def get_brief_repr(self) -> str:
        cls_name = self.__class__.__name__
        obj_name = self.get_name()
        str_args = repr(self.get_cropped_text())
        if obj_name:
            str_args += f', name={repr(obj_name)}'
        return f'{cls_name}({str_args})'

    def __str__(self):
        return self.get_text()


class Link(Text):
    def __init__(
            self,
            data: Union[str, list, None],
            url: str,
            style: OptStyle = None,
            name: str = '',
    ):
        self._url = url
        super().__init__(data=data, style=style, name=name)

    def get_url(self) -> str:
        return self._url

    def has_html_tags(self) -> bool:
        return bool(self.get_url())

    def get_html_open_tag(self) -> str:
        url = self.get_url()
        style = self.get_style()
        if Auto.is_defined(style):
            return f'<a href="{url}" style="{style}">'
        else:
            return f'<a href="{url}">'

    def get_html_close_tag(self) -> str:
        return '</a>'

    def __str__(self):
        return self.get_md_code()


class CompositionType(DynamicEnum):
    Vertical = 'vertical'
    Horizontal = 'horizontal'
    Gallery = 'gallery'


class Container(DocumentItem, IterDataMixin):
    def set_data(self, data: Items, inplace: bool, reset_dynamic_meta: bool = True, safe=True, **kwargs) -> Native:
        data = list(data)
        return super().set_data(data, inplace=inplace, reset_dynamic_meta=reset_dynamic_meta, safe=safe, **kwargs)

    def get_html_lines(self, skip_missing: bool = True) -> Iterator[str]:
        if self.has_data() or not skip_missing:
            tag = self.get_html_tag_name()
            if tag:
                yield self.get_html_open_tag()
            if self.has_data():
                for item in self.get_data():
                    if isinstance(item, DocumentItem) or hasattr(item, 'get_html_lines'):
                        yield from item.get_html_lines()
                    elif isinstance(item, str):
                        yield item
                    else:
                        raise TypeError(f'Expected item as DocumentItem or str, got item {repr(item)} as {type(item)}')
            if tag:
                yield self.get_html_close_tag()


class MultiChart(Chart, Container):
    pass


class Paragraph(Text, Container):
    def __init__(
            self,
            data: Optional[list] = None,
            level: Optional[int] = None,
            style: OptStyle = None,
            name: str = '',
    ):
        self._level = level
        super().__init__(data=data, style=style, name=name)

    def get_level(self) -> Optional[int]:
        return self._level

    def set_level(self, level: Optional[int], inplace: bool) -> Native:
        if inplace:
            self._set_level_inplace(level)
            return self
        else:
            return self._set_level_outplace(level)

    def _set_level_inplace(self, level: Optional[int]):
        self._level = level

    def _set_level_outplace(self, level: Optional[int]) -> Native:
        return Paragraph(data=self.get_data(), level=level, style=self.get_style(), name=self.get_name())

    level = property(get_level, _set_level_inplace)

    def is_title(self) -> bool:
        return (self.get_level() or 0) > 0

    def get_html_style(self) -> HtmlStyle:
        style = super().get_html_style()
        if Auto.is_defined(style):
            return style
        else:
            return H_STYLE if self.is_title() else P_STYLE

    def get_html_tag_name(self) -> str:
        level = self.get_level()
        if self.is_title():
            return f'h{level}'
        else:
            return 'p'

    def has_html_tags(self) -> bool:
        return True

    @staticmethod
    def get_html_text_code(
            lines: Iterable[str],
            level: Optional[int] = None,
            style: Union[str, Auto] = AUTO,
    ) -> Iterator[str]:
        if isinstance(lines, str):
            lines = lines.split(PARAGRAPH_CHAR)
        assert isinstance(lines, Iterable), f'got {lines}'
        text = f'<br>{PARAGRAPH_CHAR}'.join(lines)
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

    def get_brief_repr(self) -> str:
        cls_name = self.__class__.__name__
        obj_name = self.get_name()
        str_args = repr(self.get_cropped_text())
        level = self.get_level()
        if level:
            str_args += f', level={repr(level)}'
        if obj_name:
            str_args += f', name={repr(obj_name)}'
        return f'{cls_name}({str_args})'


class Chapter(Text, Container):
    def add(
            self,
            item: Union[Native, Iterable],
            before: bool = False,
            inplace: bool = False,
            **kwargs
    ) -> Native:
        assert not kwargs, f'Text.add() does not support kwargs, got {kwargs}'
        return self.add_items([item], before=before, inplace=inplace)


class Page(Container):
    pass
