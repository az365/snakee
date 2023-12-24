from typing import Optional, Iterator, Union

try:  # Assume we're a submodule in a package.
    from base.constants.chars import (
        EMPTY, DEFAULT_STR, STAR, DOT, SPACE, PARAGRAPH_CHAR, CROP_SUFFIX,
        TYPE_CHARS, TYPE_EMOJI,
    )
    from base.constants.text import DEFAULT_LINE_LEN, SHORT_LINE_LEN, EXAMPLE_STR_LEN, DEFAULT_INT_LEN
    from content.documents.document_item import DocumentItem, Text, Paragraph, Sheet, Container, CompositionType
    from content.documents.quantile_functions import (
        get_fit_line, get_empty_line, get_united_lines,
        get_compact_pair_repr, get_centred_pair_repr,
    )
    from content.documents.quantile_wrapper import (
        SimpleQuantileWrapper,
        DEFAULT_SCREEN_QUOTA, DEFAULT_CARD_QUOTA,
        KEY_DELIMITER, COLUMN_DELIMITER,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import (
        TYPE_CHARS, TYPE_EMOJI,
        EMPTY, DEFAULT_STR, STAR, DOT, SPACE, PARAGRAPH_CHAR, CROP_SUFFIX,
    )
    from ...base.constants.text import DEFAULT_LINE_LEN, SHORT_LINE_LEN, EXAMPLE_STR_LEN, DEFAULT_INT_LEN
    from .document_item import DocumentItem, Text, Paragraph, Sheet, Container, CompositionType
    from .quantile_functions import (
        get_fit_line, get_empty_line, get_united_lines,
        get_compact_pair_repr, get_centred_pair_repr,
    )
    from .quantile_wrapper import (
        SimpleQuantileWrapper,
        DEFAULT_SCREEN_QUOTA, DEFAULT_CARD_QUOTA,
        KEY_DELIMITER, COLUMN_DELIMITER,
    )

Position = int
Name = str
Focus = Union[Position, Name, None]
Collection = Union[list, set, tuple, dict]
COLLECTIONS = list, set, tuple, dict

SYMBOL_WIDTH = 7
CENTER_TEXT = 'text-align: center;'
LEFT_TEXT = 'text-align: left;'
RIGHT_TEXT = 'text-align: right;'


class AdvancedQuantileWrapper(SimpleQuantileWrapper):
    def get_fit_html_object(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
            width: Optional[int] = None
    ):
        container = self.get_fit_container(
            include_header=include_header, level=level, focus=focus, quota=quota,
            lines_count=lines_count, line_len=line_len,
            width=width,
        )
        return container.get_html_object()

    def get_fit_html_lines(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
            width: Optional[int] = None
    ):
        container = self.get_fit_container(
            include_header=include_header, level=level, focus=focus, quota=quota,
            lines_count=lines_count, line_len=line_len,
            width=width,
        )
        return container.get_html_lines()

    def get_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
    ) -> Iterator[str]:
        container = self.get_fit_container(
            include_header=include_header, level=level, focus=focus, quota=quota,
            lines_count=count, line_len=line_len,
        )
        return container.get_lines()

    def get_fit_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
            width: Optional[int] = None
    ) -> Container:
        if width is None:
            width = line_len * SYMBOL_WIDTH
        if level == 0:
            container = self.get_screen_container(
                include_header=include_header, level=level, focus=focus, quota=quota,
                lines_count=lines_count, line_len=line_len,
                width=width,
            )
        else:
            container = self.get_card_container(
                include_header=include_header, level=level, focus=focus, quota=quota,
                lines_count=lines_count, line_len=line_len,
                width=width,
            )
        return container

    def get_screen_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
            width: Optional[int] = None,
            content_composition: CompositionType = CompositionType.Horizontal,
    ) -> Container:
        if width is None:
            width = SYMBOL_WIDTH * line_len
        header_level = level + 1
        header_lines_count, content_lines_count = self.get_header_and_content_lines_count(lines_count, include_header, quota)
        container = Container([], size=width)
        if header_lines_count:
            header_container = self.get_screen_header_container(
                header_level=header_level,
                lines_count=header_lines_count, line_len=line_len,
                width=width,
            )
            container.append(header_container)
        if content_lines_count:
            content_container = self.get_content_container(
                focus=focus, composition=content_composition,
                lines_count=content_lines_count, line_len=line_len,
                width=width,
            )
            container.append(content_container)
        return container

    def get_card_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_CARD_QUOTA,  # 0.2: share of screen space allocated for focused item
            width: Optional[int] = None,
            content_composition: CompositionType = CompositionType.Horizontal,
    ) -> Container:
        if width is None:
            width = SYMBOL_WIDTH * line_len
        if content_composition is None:
            if level < 1:
                content_composition = CompositionType.Horizontal
            else:
                content_composition = CompositionType.Vertical
        header_lines_count, content_lines_count = self.get_header_and_content_lines_count(lines_count, include_header, quota)
        container = Container([], size=width)
        if header_lines_count:
            header_container = self.get_card_header_container(
                level=level + 1,
                lines_count=header_lines_count, line_len=line_len,
                width=width,
            )
            container.append(header_container)
        if content_lines_count:
            if line_len > SHORT_LINE_LEN:
                content_container = self.get_wide_content_container(
                    focus=focus, composition=content_composition,
                    lines_count=content_lines_count, line_len=line_len,
                    width=width,
                )
            else:
                content_container = self.get_narrow_content_container(
                    lines_count=content_lines_count, line_len=line_len,
                    width=width,
                )
            container.append(content_container)
        return container

    def get_wide_content_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            quota: float = DEFAULT_CARD_QUOTA,  # 0.2: share of screen space allocated for focused item
            level: int = 0,
            focus: Focus = None,
            composition: Optional[CompositionType] = None,
            width: Optional[int] = None,
    ) -> Container:
        title_lines_count = int(lines_count * quota)
        if title_lines_count < 1:
            title_lines_count = 1
        items_lines_count = lines_count - title_lines_count
        title_container = self.get_title_container(lines_count, line_len=line_len, level=level, width=width)
        container = Container([title_container], name='wide content container', size=width)
        if items_lines_count:
            items_container = self.get_content_container(
                focus=focus, composition=composition,
                lines_count=items_lines_count, line_len=line_len,
                width=width,
            )
            container.append(items_container)
        return container

    def get_narrow_content_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            quota: float = DEFAULT_CARD_QUOTA,  # 0.2: share of screen space allocated for focused item
            level: int = 0,
            width: Optional[int] = None,
    ) -> Container:
        value_lines_count = int(lines_count * quota)
        if value_lines_count < 1:
            value_lines_count = 1
        props_lines_count = lines_count - value_lines_count
        brief_value_container = self.get_brief_value_container(lines_count, line_len=line_len, level=level, width=width)
        container = Container([brief_value_container], name='narrow content container', size=width)
        if props_lines_count:
            props_container = self.get_props_container(props_lines_count, line_len=line_len)
            container.append(props_container)
        return container

    def get_title_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
            level: int = 0,
            width: Optional[int] = None,
    ) -> Container:
        title_lines_count = 1
        space_lines_count = lines_count - title_lines_count
        if width is None:
            width = SYMBOL_WIDTH * line_len
        brief_value_line_len = int(line_len * quota)
        brief_value_width = int(width * quota)
        class_line_len = int((line_len - brief_value_line_len) / 2)
        class_width = int((width - brief_value_width) / 2)
        name_line_len = line_len - class_line_len - brief_value_line_len
        name_width = width - class_width - brief_value_width
        assert line_len == class_line_len + brief_value_line_len + name_line_len
        assert width == class_width + brief_value_width + name_width
        brief_value_container = self.get_brief_value_container(
            level=level,
            lines_count=lines_count, line_len=brief_value_line_len,
            width=brief_value_width,
        )
        class_container = self.get_class_container(
            level=level,
            lines_count=lines_count, line_len=class_line_len,
            width=class_width,
        )
        name_container = self.get_name_container(
            level=level,
            lines_count=lines_count, line_len=name_line_len,
            width=name_width,
        )
        items = class_container, brief_value_container, name_container
        container = Container(items, name='title container', composition=CompositionType.Horizontal, size=width)
        if space_lines_count:
            container = Container([container], 'title container+', composition=CompositionType.Vertical, size=width)
            empty_line = get_empty_line(line_len)
            empty_paragraph = Paragraph([empty_line], name='empty line')
            for _ in range(space_lines_count):
                container.append(empty_paragraph)
        return container

    def get_brief_value_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            level: int = 0,
            width: Optional[int] = None,
    ) -> Container:
        container = Container([], name='brief value container', size=width)
        if lines_count > 0:
            brief_value = self.get_value_text_repr(line_len=line_len)
            paragraph = Paragraph([brief_value], level=level + 1, style=CENTER_TEXT, name='brief value')
            container.append(paragraph)
            for i in range(lines_count - 1):
                paragraph = Paragraph(EMPTY, name='indent')
                container.append(paragraph)
        return container

    def get_class_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            level: int = 0,
            width: Optional[int] = None,
            style: str = 'text-align: right;'
    ) -> Container:
        container = Container([], name='class container', size=width)
        if lines_count > 0:
            class_name = self.get_class_name()
            class_text = get_fit_line(class_name, line_len=line_len, align_left=False, align_right=True)
            paragraph = Paragraph([class_text], level=level + 1, style=style, name='class')
            container.append(paragraph)
            for i in range(lines_count - 1):
                paragraph = Paragraph(EMPTY, name='indent')
                container.append(paragraph)
        return container

    def get_name_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            level: int = 0,
            width: Optional[int] = None,
            style: str = LEFT_TEXT,
    ) -> Container:
        container = Container([], name='name container', size=width)
        if lines_count > 0:
            obj_name = self.get_visible_name()
            class_text = get_fit_line(obj_name, line_len=line_len, align_left=False, align_right=True)
            paragraph = Paragraph([class_text], level=level + 1, style=style, name='name')
            container.append(paragraph)
            for i in range(lines_count - 1):
                paragraph = Paragraph(EMPTY, name='indent')
                container.append(paragraph)
        return container

    def get_content_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            focus: Focus = None,
            composition: Optional[CompositionType] = CompositionType.Horizontal,
            width: Optional[int] = None,
    ) -> Container:
        if width is None:
            width = line_len * SYMBOL_WIDTH
        if composition is None:
            composition = CompositionType.Horizontal,
        items = self.get_content_items(lines_count=lines_count, line_len=line_len, focus=focus)
        return Container(items, composition=composition, size=width, name='content container')

    def get_content_items(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            focus: Focus = None,
    ) -> Iterator[Container]:
        is_collection = isinstance(self.obj, COLLECTIONS) and not isinstance(self.obj, str)
        is_empty = self.obj is None
        if is_collection and line_len > SHORT_LINE_LEN:  # 30
            yield from self.get_collection_items(count=lines_count, line_len=line_len, focus=focus)
        elif is_empty:
            for _ in range(lines_count):
                line = get_empty_line(line_len)
                yield Paragraph([line])
        else:
            assert not focus
            yield self.get_props_container(count=lines_count, line_len=line_len)

    def get_card_header_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            level: int = 1,
            width: Optional[int] = None,
    ) -> Container:
        if width is None:
            width = line_len * SYMBOL_WIDTH
        prop_name = self.prop
        prop_text = get_fit_line(prop_name, line_len=line_len, centred=True, align_left=True, align_right=True)
        prop_paragraph = Paragraph([prop_text], level=level + 2, style=CENTER_TEXT)
        items = [prop_paragraph]
        if lines_count > 1:  # TMP
            for i in range(lines_count - 1):
                items.append(Paragraph(EMPTY))
        return Container(items, name=f'{prop_name} card title', size=width)

    def get_screen_header_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            header_level: int = 0,
            width: Optional[int] = None,
    ) -> Container:
        if width is None:
            width = line_len * SYMBOL_WIDTH
        items = self.get_screen_header_items(lines_count=lines_count, line_len=line_len, header_level=header_level)
        return Container(items, name=f'screen header', size=width)

    def get_screen_header_items(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            header_level: int = 0,
    ) -> Iterator[Container]:
        if lines_count >= 3:
            yield self.get_parents_container(lines_count=lines_count - 2, line_len=line_len, header_level=header_level)
        if lines_count >= 1:
            parent = self.parent
            if parent and header_level > 0:
                if not isinstance(parent, QuantileWrapper):
                    parent = QuantileWrapper(parent)
                focused_items = parent.get_focused_collection_items(
                    count=1, line_len=line_len, items_count=3,
                    focus=self.prop or self.get_visible_name(),
                    is_header=True,
                    quota=0.5,
                )
                yield Container(focused_items, composition=CompositionType.Horizontal, name='main header')
            else:
                title_text = self.get_title_text_repr(line_len=line_len, delimiter=None)
                title_paragraph = Paragraph([title_text], style=CENTER_TEXT, level=header_level + 1)
                yield title_paragraph
        if lines_count >= 2:
            empty_line = get_empty_line(line_len)
            empty_paragraph = Paragraph([empty_line])
            yield empty_paragraph

    def get_parents_container(
            self,
            lines_count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            header_level: int = 0,
    ) -> Container:
        container = Container([], name='parents')
        parent = self.parent
        if parent is None:
            substitute = DOT
            for _ in range(lines_count):
                line = get_fit_line(substitute, line_len=line_len, align_left=True, align_right=True)
                paragraph = Paragraph([line], style=CENTER_TEXT)
                container.append(paragraph)
        else:
            if not isinstance(parent, QuantileWrapper):
                parent = QuantileWrapper(parent)
            if lines_count > 1:
                second_level_parents_container = parent.get_parents_container(lines_count - 1, line_len=line_len, header_level=header_level + 2)
                container.append(second_level_parents_container)
            title_text = parent.get_title_text_repr(line_len=line_len, delimiter=None)
            paragraph = Paragraph([title_text], style=CENTER_TEXT, level=header_level + 1)
            container.append(paragraph)
        return container

    def get_collection_items(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            max_items: int = 5,
            focus: Focus = None,
    ) -> Iterator[DocumentItem]:
        items_count = len(self.obj)
        if max_items < items_count:
            items_count = max_items
        if focus is None:
            yield from self.get_uniform_collection_items(count, line_len=line_len, items_count=items_count)
        else:
            yield from self.get_focused_collection_items(count, line_len=line_len, focus=focus)

    def get_uniform_collection_items(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            items_count: int = 5,
            include_header: Optional[bool] = None,
            delimiter: str = COLUMN_DELIMITER,
            level: int = 0,
            content_composition: CompositionType = None,
    ) -> Iterator[DocumentItem]:
        delimiter_len = len(delimiter)
        item_len = int((line_len - delimiter_len) / items_count) - delimiter_len
        assert item_len > 0
        if content_composition is None:
            content_composition = CompositionType.Horizontal
        collection = self.get_list_items()
        for i_no, i in enumerate(collection[:items_count]):
            assert isinstance(i, AdvancedQuantileWrapper)
            yield i.get_card_container(
                include_header=include_header, level=level - 1,
                content_composition=content_composition,
                lines_count=count, line_len=item_len,
            )

    def get_focused_collection_items(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,  # 120
            items_count: int = 5,
            focus: Focus = None,
            is_header: bool = False,
            include_header: Optional[bool] = None,
            delimiter: str = COLUMN_DELIMITER,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33 or 0.5
            width: Optional[int] = None
    ) -> Iterator[DocumentItem]:
        yield from self.get_uniform_collection_items(count, line_len, items_count, include_header, delimiter)
        if width is None:
            width = line_len * SYMBOL_WIDTH
        assert items_count % 2 == 1, items_count
        available_items_count = len(self.obj)
        list_items = self.get_list_items()
        additions_count = int((items_count - 1) / 2)  # left and right items around focused item
        focused_no = None
        if isinstance(focus, int):
            focused_no = focus
        else:
            for n, i in enumerate(list_items):
                if i.prop == focus:
                    focused_no = n
                    break
        props = [i.prop for i in list_items]
        assert focused_no is not None, f'key {repr(focus)} not found (available are: {props})'
        focused_line_len = int(line_len * quota)
        current_line_len = focused_line_len
        focused_item = list_items[focused_no]
        i = focused_item
        assert isinstance(i, SimpleQuantileWrapper)
        if is_header:
            lines = i.get_header_fit_text_lines(count=count, line_len=focused_line_len)
        else:
            lines = i.get_fit_text_lines(line_len=focused_line_len, count=count)
        for step_n in range(1, additions_count + 1):
            current_line_len = int(current_line_len * quota)
            for is_left, sign in zip((True, False), (-1, 1)):
                item_n = focused_no + step_n * sign
                if 0 <= item_n < available_items_count:
                    i = list_items[item_n]
                    assert isinstance(i, SimpleQuantileWrapper)
                else:
                    i = self.get_empty_item()
                if is_header:
                    addition = i.get_header_fit_text_lines(count=count, line_len=focused_line_len)
                else:
                    addition = i.get_fit_text_lines(line_len=current_line_len, count=count)
                lines = get_united_lines(lines, addition, invert=is_left, delimiter=delimiter)

    def get_props_container(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            delimiter: str = KEY_DELIMITER,
            width: Optional[int] = None,
    ) -> Container:
        if width is None:
            width = line_len * SYMBOL_WIDTH
        delimiter_len = len(delimiter) - 2
        delimiter_width = delimiter_len * SYMBOL_WIDTH
        text_width = int((width - delimiter_width) / 2)
        container = Container([], name='props container', style='padding: 0, spacing: 0')
        n = 0
        for key, value in self.get_prop_pairs():
            items = [
                Container([Paragraph([key], style=RIGHT_TEXT)], size=text_width),
                Container([Paragraph([delimiter], style=CENTER_TEXT)], size=delimiter_width),
                Container([Paragraph([value], style=LEFT_TEXT)], size=text_width),
            ]
            row = Container(items, composition=CompositionType.Horizontal, size=width, style='padding: 0; spacing: 0;')
            container.append(row)
            n += 1
            if n >= count:
                break
        substitute = DOT
        while n < count:
            empty_line = get_fit_line(substitute, line_len=line_len, align_left=True, align_right=True)
            container.append(Paragraph([empty_line], style=CENTER_TEXT))
            n += 1
        return container


QuantileWrapper = AdvancedQuantileWrapper
