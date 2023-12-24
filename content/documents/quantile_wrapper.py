from abc import ABC
from typing import Optional, Iterator, Union

try:  # Assume we're a submodule in a package.
    from base.constants.chars import (
        EMPTY, DEFAULT_STR, STAR, DOT, SPACE, PARAGRAPH_CHAR, CROP_SUFFIX,
        TYPE_CHARS, TYPE_EMOJI,
    )
    from base.constants.text import DEFAULT_LINE_LEN, SHORT_LINE_LEN, EXAMPLE_STR_LEN, DEFAULT_INT_LEN
    from content.documents.quantile_functions import (
        get_fit_line, get_empty_line, get_united_lines,
        get_compact_pair_repr, get_centred_pair_repr,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import (
        TYPE_CHARS, TYPE_EMOJI,
        EMPTY, DEFAULT_STR, STAR, DOT, SPACE, PARAGRAPH_CHAR, CROP_SUFFIX,
    )
    from ...base.constants.text import DEFAULT_LINE_LEN, SHORT_LINE_LEN, EXAMPLE_STR_LEN, DEFAULT_INT_LEN
    from .quantile_functions import (
        get_fit_line, get_empty_line, get_united_lines,
        get_compact_pair_repr, get_centred_pair_repr,
    )

Position = int
Name = str
Focus = Union[Position, Name, None]
Collection = Union[list, set, tuple, dict]
COLLECTIONS = list, set, tuple, dict

TYPING_DELIMITER = ' * '
KEY_DELIMITER = ' : '
COLUMN_DELIMITER = ' | '
PATH_DELIMITER = ' > '

DEFAULT_SCREEN_QUOTA = 0.33
DEFAULT_CARD_QUOTA = 0.2


class QuantileWrapperInterface(ABC):
    def get_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
    ) -> Iterator[str]:
        pass


class AbstractQuantileWrapper(QuantileWrapperInterface, ABC):
    obj = None
    parent = None
    prop = None

    def __init__(self, obj, parent=None, prop=None):
        self.obj = obj
        self.parent = parent
        self.prop = prop

    @classmethod
    def get_empty_item(cls):
        return cls(None)

    def set_obj(self, obj):
        self.obj = obj
        return self

    # @deprecated
    def show(self, lines_count: int, line_len: int = DEFAULT_LINE_LEN):
        self.print(lines_count=lines_count, line_len=line_len)
        return self

    def print(self, lines_count: int = 1, line_len: int = 10, include_header: bool = False, level: int = 0):
        text_repr = self.get_fit_text_str(lines_count, line_len=line_len, include_header=include_header, level=level)
        print(text_repr)
        return self

    def get_fit_text_str(self, count: int = 1, line_len: int = 10, include_header: bool = False, level: int = 0) -> str:
        count = self.get_fit_text_lines(count=count, line_len=line_len, include_header=include_header, level=level)
        return PARAGRAPH_CHAR.join(count)

    def get_class_name(self) -> str:
        return self.obj.__class__.__name__

    def get_visible_name(self, use_prop: bool = True):
        if hasattr(self.obj, 'get_name'):
            name = self.obj.get_name()
        elif self.prop and use_prop:
            name = self.prop
        elif isinstance(self.obj, str):
            name = self.obj
        elif isinstance(self.obj, COLLECTIONS):
            count = self.get_visible_count()
            items_name = self.get_items_name()
            name = f'{count} {items_name}'
        else:
            name = str(self.obj)
        return name

    def get_visible_count(self) -> int:
        if hasattr(self.obj, 'get_count'):
            return self.obj.get_count()
        elif isinstance(self.obj, COLLECTIONS):
            return len(self.obj)
        else:
            return len(str(self.obj))  # or None ?

    def get_items_name(self):
        if isinstance(self.obj, (int, float)):
            item_name = 'digit'
        elif isinstance(self.obj, str):
            item_name = 'symbol'
        elif isinstance(self.obj, COLLECTIONS):
            item_name = 'item'
        else:
            item_name = 'symbol'
        if self.get_visible_count() == 1:
            return item_name
        else:
            return f'{item_name}s'

    def get_list_items(self) -> list:
        return list(self.get_paired_items())

    def get_paired_items(self) -> Iterator[QuantileWrapperInterface]:
        cls = self.__class__
        if isinstance(self.obj, dict):
            for k, v in self.obj.items():
                yield cls(prop=k, obj=v)
        elif isinstance(self.obj, (list, tuple)):
            for n, i in enumerate(self.obj):
                yield cls(prop=f'#{n}', obj=i)
        elif isinstance(self.obj, set):
            for i in self.obj:
                yield cls(prop=i.__class__.__name__, obj=i)
        else:
            raise TypeError(f'got {self.obj}')

    def get_paired_props(self) -> Iterator[QuantileWrapperInterface]:
        cls = self.__class__
        for k, v in self.get_paired_props():
            yield cls(prop=k, obj=v)

    def get_prop_pairs(self) -> Iterator[tuple]:
        yield 'class', self.get_class_name()
        if isinstance(self.obj, (str, int)):
            yield 'len', self.get_count_text_repr(centred=False)
        elif isinstance(self.obj, (set, list, tuple, dict)):
            yield 'count', self.get_count_text_repr(centred=False)
        if hasattr(self.obj, '__dict__'):
            for k, v in self.obj.__dict__.items():
                yield k, v

    def get_type_delimiter(self, use_emoji: bool = False, default: str = STAR, add_spaces: bool = True) -> str:
        type_name = self.get_class_name()
        if use_emoji:
            char = TYPE_EMOJI.get(type_name, default)
        else:
            char = TYPE_CHARS.get(type_name, default)
        if add_spaces:
            return f' {char} '
        else:
            return char


class SimpleQuantileWrapper(AbstractQuantileWrapper):
    def get_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            include_header: bool = None,  # Auto
            level: int = 0,
            focus: Focus = None,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33: share of screen space allocated for focused item
    ) -> Iterator[str]:
        header_level = level + 1
        if include_header or include_header is None:
            header_rows_count = int(count * quota)
            if include_header and not header_rows_count:
                header_rows_count = 1
        else:  # include_header == False
            header_rows_count = 0
        content_rows_count = count - header_rows_count
        if header_rows_count:
            yield from self.get_header_fit_text_lines(header_rows_count, line_len=line_len, header_level=header_level)
        if content_rows_count:
            yield from self.get_content_fit_text_lines(content_rows_count, line_len=line_len, focus=focus)

    def get_content_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            focus: Focus = None,
    ) -> Iterator[str]:
        is_collection = isinstance(self.obj, COLLECTIONS) and not isinstance(self.obj, str)
        is_empty = self.obj is None
        if is_collection and line_len > SHORT_LINE_LEN:  # 30
            yield from self.get_collection_fit_text_lines(count=count, line_len=line_len, focus=focus)
        elif is_empty:
            for _ in range(count):
                yield get_empty_line(line_len)
        else:
            assert not focus
            if count >= 1:
                yield self.get_value_text_repr(line_len, align_left=True, align_right=True)
            if count >= 2:
                yield from self.get_props_fit_text_lines(count=count - 1, line_len=line_len)

    def get_collection_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            max_items: int = 5,
            focus: Focus = None,
            axis: Optional[int] = None,
    ) -> Iterator[str]:
        items_count = len(self.obj)
        if max_items < items_count:
            items_count = max_items
        if axis is None:
            if focus is None and (items_count <= count) and (line_len <= SHORT_LINE_LEN):  # 30
                axis = 0
            else:
                axis = 1
        if axis == 0:
            yield from self.get_vertical_collection_fit_text_lines(count=count, line_len=line_len)
        elif axis == 1:
            if focus is None:
                yield from self.get_uniform_collection_fit_text_lines(count, line_len=line_len, items_count=items_count)
            else:
                yield from self.get_focused_collection_fit_text_lines(count, line_len=line_len, focus=focus)
        else:
            raise ValueError(f'axis-argument must be 0 or 1, got {axis}')

    def get_vertical_collection_fit_text_lines(self, count: int = 1, line_len: int = DEFAULT_LINE_LEN) -> Iterator[str]:
        if isinstance(self.obj, set):
            delimiter = TYPING_DELIMITER
        else:
            delimiter = KEY_DELIMITER
        centred = True
        if isinstance(self.obj, (list, tuple)):
            items_count = len(self.obj)
            for n, i in enumerate(self.obj):
                if len(str(i)) >= line_len / 2:
                    centred = False
                if centred or n >= min(count, items_count):
                    break
        for n, i in enumerate(self.get_paired_items()):
            if n >= count:
                break
            assert isinstance(i, QuantileWrapper), i
            k = i.prop
            v = i.obj
            if centred:
                yield get_centred_pair_repr(k, v, delimiter=delimiter, line_len=line_len)
            else:
                yield get_compact_pair_repr(k, v, delimiter=delimiter, line_len=line_len)

    def get_uniform_collection_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            items_count: int = 5,
            include_header: Optional[bool] = None,
            delimiter: str = COLUMN_DELIMITER,
            level: int = 0,
    ) -> Iterator[str]:
        delimiter_len = len(delimiter)
        item_len = int((line_len - delimiter_len) / items_count) - delimiter_len
        assert item_len > 0
        lines = [EMPTY] * count

        collection = self.get_list_items()
        for i_no, i in enumerate(collection[:items_count]):
            i_wrapped = i
            item_lines = i_wrapped.get_fit_text_lines(count, item_len, include_header=include_header, level=level - 1)
            for line_no, line in enumerate(item_lines):
                line = get_fit_line(line, line_len=item_len, delimiter=DOT, align_left=True, align_right=True)
                lines[line_no] += delimiter
                lines[line_no] += line
        for i in lines:
            yield get_fit_line(i + delimiter, line_len, align_left=True, align_right=True)

    def get_focused_collection_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,  # 120
            items_count: int = 5,
            focus: Focus = None,
            is_header: bool = False,
            delimiter: str = COLUMN_DELIMITER,
            quota: float = DEFAULT_SCREEN_QUOTA,  # 0.33 or 0.5
    ) -> Iterator[str]:
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
        for line in lines:
            yield get_fit_line(line, line_len=line_len, align_left=True, align_right=True)

    def get_header_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            header_level: int = 0,
    ) -> Iterator[str]:
        if count >= 3:
            yield from self.get_parents_fit_text_lines(count=count - 2, line_len=line_len, header_level=header_level)
        if count >= 1:
            parent = self.parent
            if parent and header_level > 0:
                if not isinstance(parent, QuantileWrapper):
                    parent = QuantileWrapper(parent)
                focused_text_lines = parent.get_focused_collection_fit_text_lines(
                    count=1, line_len=line_len, items_count=3,
                    focus=self.prop or self.get_visible_name(),
                    is_header=True,
                    quota=0.5,
                )
                yield from focused_text_lines
            else:
                yield self.get_title_text_repr(line_len=line_len, delimiter=None)
        if count >= 2:
            yield get_empty_line(line_len)

    def get_parents_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            header_level: int = 0,
    ) -> Iterator[str]:
        parent = self.parent
        if parent is None:
            substitute = DOT
            for _ in range(count):
                yield get_fit_line(substitute, line_len=line_len, align_left=True, align_right=True)
        else:
            if not isinstance(parent, QuantileWrapper):
                parent = QuantileWrapper(parent)
            if count > 1:
                yield from parent.get_parents_fit_text_lines(count - 1, line_len, header_level=header_level + 2)
            yield parent.get_title_text_repr(line_len=line_len, delimiter=None)

    def get_path_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            delimiter: str = PATH_DELIMITER,
    ) -> Iterator[str]:
        raise NotImplementedError

    def get_props_fit_text_lines(
            self,
            count: int = 1,
            line_len: int = DEFAULT_LINE_LEN,
            delimiter: str = KEY_DELIMITER,
    ) -> Iterator[str]:
        n = 0
        for key, value in self.get_prop_pairs():
            yield get_centred_pair_repr(key, value, delimiter=delimiter, line_len=line_len)
            n += 1
            if n >= count:
                break
        substitute = DOT
        if n < count:
            yield get_fit_line(substitute, line_len=line_len, align_left=True, align_right=True)
            n += 1

    def get_one_line_text_repr(self, line_len: int = DEFAULT_LINE_LEN) -> str:
        if line_len < EXAMPLE_STR_LEN:
            return self.get_value_text_repr(line_len, align_left=True, align_right=True)
        else:
            return self.get_title_text_repr(line_len, delimiter=self.get_type_delimiter())

    def get_title_text_repr(
            self,
            line_len: int = DEFAULT_LINE_LEN,  # 120
            delimiter: Optional[str] = None,  # use default delimiter
            len_for_name: int = DEFAULT_INT_LEN,  # 7
    ) -> str:
        if self.prop:
            return get_fit_line(self.prop, line_len=line_len, align_left=True, align_right=True)
        if self.obj is None:
            return get_fit_line(DEFAULT_STR, line_len=line_len, align_left=True, align_right=True)
        elif line_len <= len_for_name:
            return self.get_name_text_repr(line_len=line_len, align_left=True, align_right=True)
        else:
            if delimiter is None:
                delimiter = self.get_type_delimiter()
            class_name = self.get_class_name()
            obj_name = self.get_visible_name()
            return get_centred_pair_repr(class_name, obj_name, delimiter=delimiter, line_len=line_len)

    def get_count_text_repr(
            self,
            line_len: int = DEFAULT_LINE_LEN,  # 120
            delimiter: str = SPACE,  # ' '
            len_for_name: int = DEFAULT_INT_LEN,  # 7
            centred: bool = True,
    ) -> str:
        items_count_str = self.get_visible_count()
        if line_len < len_for_name:
            return get_fit_line(items_count_str, line_len=line_len, align_left=True, align_right=centred)
        else:
            items_name = self.get_items_name()
            if centred:
                return get_centred_pair_repr(items_count_str, items_name, delimiter=delimiter, line_len=line_len)
            else:
                return get_compact_pair_repr(items_count_str, items_name, delimiter=delimiter, line_len=line_len)

    def get_name_text_repr(
            self,
            line_len: int = DEFAULT_LINE_LEN,
            align_left=False,
            align_right=False,
    ) -> str:
        name = self.get_visible_name()
        return get_fit_line(name, line_len, align_left=align_left, align_right=align_right)

    def get_value_text_repr(self, line_len: int = DEFAULT_LINE_LEN, align_left=False, align_right=False) -> str:
        value = repr(self.obj)
        return get_fit_line(value, line_len, align_left=align_left, align_right=align_right)

    def get_class_text_repr(self, line_len: int = DEFAULT_LINE_LEN, align_left=False, align_right=False):
        class_name = self.get_class_name()
        return get_fit_line(class_name, line_len, align_left=align_left, align_right=align_right)


QuantileWrapper = SimpleQuantileWrapper
