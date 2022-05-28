from typing import Optional, Iterable, Iterator, Tuple, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto
    from base.classes.enum import DynamicEnum
    from base.constants.chars import SPACE, PY_INDENT
    from base.abstract.simple_data import SimpleDataWrapper, SimpleDataInterface
    from base.mixin.iter_data_mixin import IterDataMixin
    from base.mixin.map_data_mixin import MapDataMixin
    from functions.primary.items import get_fields_values_from_item, get_field_value_from_item
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from util.external import HTML
    from content.items.item_type import ItemType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import AUTO, Auto
    from ...base.classes.enum import DynamicEnum
    from ...base.constants.chars import SPACE, PY_INDENT
    from ...base.abstract.simple_data import SimpleDataWrapper, SimpleDataInterface
    from ...base.mixin.iter_data_mixin import IterDataMixin
    from ...base.mixin.map_data_mixin import MapDataMixin
    from ...functions.primary.items import get_fields_values_from_item, get_field_value_from_item
    from ...streams.interfaces.regular_stream_interface import RegularStreamInterface
    from ...utils.external import HTML
    from .item_type import ItemType

HtmlStyle = str
ContentStyle = Any
Style = Union[HtmlStyle, ContentStyle]
OptStyle = Optional[Style]

H_STYLE = None
P_STYLE = 'line-height: 1.1em; margin-top: 0em; margin-bottom: 0em; padding-top: 0em; padding-bottom: 0em;'
HTML_SPACE = '&nbsp;'
HTML_INDENT = PY_INDENT


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
        elif isinstance(data, Iterable):
            yield from data
        elif hasattr(data, 'get_lines'):  # isinstance(data, RegularStream)
            yield from data.get_lines()
        elif data:
            raise TypeError(f'Expected str, Iterable or Stream, got {data}')

    def get_text(self) -> str:
        data = self.get_data()
        if isinstance(data, str):
            return data
        elif isinstance(data, Iterable):
            return '\n'.join(data)

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
        else:
            return ''

    def get_html_close_tag(self) -> str:
        tag = self.get_html_tag_name()
        attributes = self.get_html_attr_str()
        if attributes:
            return f'</{tag}>'
        else:
            return ''

    def get_items_html_lines(self) -> Iterator[str]:
        for item in self.get_items():
            if isinstance(item, str):
                yield item
            elif isinstance(item, DocumentItem) or hasattr(item, 'get_items_html_lines'):
                yield from item.get_items_html_lines()
            elif hasattr(item, 'get_lines'):  # isinstance(item, RegularStream)
                yield from item.get_lines()
            elif isinstance(item, Iterable):
                yield from item
            else:
                raise TypeError(f'Expected item as DocumentItem or str, got item {item}')

    def get_html_lines(self) -> Iterator[str]:
        if self.has_html_tags():
            yield self.get_html_open_tag()
        yield from self.get_items_html_lines()
        if self.has_html_tags():
            yield self.get_html_close_tag()

    def get_md_lines(self) -> Iterator[str]:
        yield from self.get_lines()

    def get_html_code(self) -> str:
        return '\n'.join(self.get_html_lines())

    def get_html_object(self):
        return HTML(self.get_html_code())


class Sheet(DocumentItem, IterDataMixin):
    def __init__(self, data: RegularStreamInterface, name: str = ''):
        super().__init__(data=data, name=name)

    def get_data(self) -> RegularStreamInterface:
        data = super().get_data()
        assert isinstance(data, RegularStreamInterface), f'got {data}'
        return data

    def get_columns(self) -> list:
        return list(self.get_data().get_columns())

    def get_records(self) -> Iterable:
        return self.get_data().get_records()

    def get_rows(self) -> Iterator:
        return self.get_data().get_rows()

    def get_formatted_rows(self) -> Iterator[str]:
        return self.get_rows()

    def get_lines(self) -> str:
        return self.get_data().get_lines()

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
            yield (HTML_INDENT * 2) + f'<th>{c}</th>'
        yield HTML_INDENT + '</tr>'
        yield f'</{tag}>'

    def get_items_html_lines(self, count: Optional[int] = None) -> Iterator[str]:
        style = self.get_html_style()
        for n, row in enumerate(self.get_formatted_rows()):
            yield '<tr>'
            for cell in row:
                if Auto.is_defined(style):
                    yield HTML_INDENT + f'<td>{cell}</td>'
                else:
                    yield HTML_INDENT + f'<td style="{style}">{cell}</td>'
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


class Chart(DocumentItem):
    pass


class Text(DocumentItem, IterDataMixin):
    def __init__(
            self,
            data: Union[str, list, None],
            style: OptStyle = None,
            name: str = '',
    ):
        self._style = style
        super().__init__(data=data, name=name)

    def get_items_html_lines(self, add_br: bool = True, split_br: bool = True) -> Iterator[str]:
        for n, i in enumerate(super().get_items_html_lines()):
            if split_br:
                lines = i.split('\n')
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


class CompositionType(DynamicEnum):
    Vertical = 'vertical'
    Horizontal = 'horizontal'
    Gallery = 'gallery'


class Container(DocumentItem, IterDataMixin):
    pass


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
