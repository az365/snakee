from typing import Optional, Callable, Iterable, Generator, Iterator, Sequence, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoCount, Count, Class
    from base.functions.arguments import get_name, get_value, get_str_from_args_kwargs
    from base.constants.chars import (
        REPR_DELIMITER, SMALL_INDENT, MD_HEADER, PARAGRAPH_CHAR, ITEM, EMPTY,
        DEFAULT_LINE_LEN,
    )
    from base.interfaces.base_interface import BaseInterface
    from base.interfaces.display_interface import DisplayInterface, Item, AutoStyle, AutoDisplay, DEFAULT_EXAMPLE_COUNT
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoCount, Count, Class
    from ..functions.arguments import get_name, get_value, get_str_from_args_kwargs
    from ..constants.chars import (
        REPR_DELIMITER, SMALL_INDENT, MD_HEADER, PARAGRAPH_CHAR, ITEM, EMPTY,
        DEFAULT_LINE_LEN,
    )
    from ..interfaces.base_interface import BaseInterface
    from ..interfaces.display_interface import DisplayInterface, Item, AutoStyle, AutoDisplay, DEFAULT_EXAMPLE_COUNT
    from ...utils.decorators import deprecated_with_alternative

DEFAULT_INT_WIDTH, DEFAULT_FLOAT_WIDTH = 7, 12
COLS_FOR_META = ('defined', 3), ('key', 20), ('value', 30), ('actual_type', 14), ('expected_type', 20), ('default', 20)
DEFAULT_CHAPTER_TITLE_LEVEL = 3


class DefaultDisplay(DisplayInterface):
    _display: DisplayInterface
    _sheet_class: Class = None

    def get_display(self, display: AutoDisplay = AUTO) -> DisplayInterface:
        if isinstance(display, DisplayInterface):
            return display
        elif not Auto.is_defined(display):
            if hasattr(self, '_display'):
                display = self._display
            if Auto.is_defined(display):
                return display
            else:
                return self
        else:
            raise TypeError(f'expected Display, got {display}')

    def set_display(self, display: DisplayInterface) -> DisplayInterface:
        self._set_display_inplace(display)
        return self

    def _set_display_inplace(self, display: DisplayInterface) -> None:
        self._display = display

    display = property(get_display, _set_display_inplace)

    @deprecated_with_alternative('get_display()')
    def get_output(self, output: AutoDisplay = AUTO) -> DisplayInterface:
        return self.get_display(output)

    # @deprecated_with_alternative('display_item()')
    def append(self, text: str) -> None:
        self.display_item(text)

    @classmethod
    def build_sheet(cls, records: Iterable[dict], columns: list):
        sheet_class = cls.get_sheet_class()
        assert sheet_class, '_sheet_class property must be defined for build sheet, use Display.set_sheet_class()'
        return sheet_class.from_records(records, columns=columns)

    @staticmethod
    def build_paragraph(data: Iterable, level: Count = 0, name: str = EMPTY):
        if isinstance(data, str):
            data = [data]
        text = ''
        for line in data:
            if level:
                if level > 0:
                    prefix = MD_HEADER * level
                elif level < 0:
                    prefix = SMALL_INDENT * (-1 - level) + ITEM
                line = f'{prefix} {text}'
            text = text + line + PARAGRAPH_CHAR
        return text

    @deprecated_with_alternative('display_item(build_paragraph())')
    def display_paragraph(
            self,
            paragraph: Optional[Iterable] = None,
            level: Optional[int] = None,
            style: AutoStyle = AUTO,
    ) -> None:
        if paragraph:
            if isinstance(paragraph, str):
                return self.display_item(paragraph)
            elif isinstance(paragraph, Iterable):
                for line in paragraph:
                    return self.display_item(line)
            else:
                raise TypeError(f'Expected paragraph as Paragraph, str or Iterable, got {paragraph}')

    @deprecated_with_alternative('display_item(Sheet.from_records())')
    def display_sheet(self, records: Iterable, columns: Sequence, name: str = EMPTY) -> None:
        sheet_class = self.get_sheet_class()
        if sheet_class:
            assert hasattr(sheet_class, 'from_records'), sheet_class  # isinstance(sheet_class, SheetInterface)
            sheet = sheet_class.from_records(records, columns=columns, name=name)
        else:
            raise AssertionError('_sheet_class property must be defined for build sheet, use Display.set_sheet_class()')
        self.display_item(sheet)

    def display_item(self, item: Item, item_type='paragraph', **kwargs) -> None:
        item_type_value = get_value(item_type)
        method_name = f'display_{item_type_value}'
        method = getattr(self, method_name, self.display_paragraph)
        return method(item, **kwargs)

    @classmethod
    def get_header_chapter_for(cls, obj, level: int = 1, comment: str = EMPTY) -> Iterable:
        if hasattr(obj, 'get_str_title'):
            title = obj.get_str_title()
        else:
            title = get_name(obj)
        yield cls.build_paragraph(title, level=level)
        if comment:
            yield cls.build_paragraph(comment)
        if hasattr(obj, 'get_str_headers'):
            yield cls.build_paragraph(obj.get_str_headers())

    @classmethod
    def get_meta_sheet_for(cls, obj, name: str = 'MetaInformation sheet'):
        sheet_class = cls.get_sheet_class()
        if sheet_class and (isinstance(obj, BaseInterface) or hasattr(obj, 'get_meta_records')):
            meta_records = obj.get_meta_records()
            assert hasattr(sheet_class, 'from_records'), sheet_class  # isinstance(sheet_class, SheetInterface)
            return sheet_class.from_records(meta_records, columns=COLS_FOR_META, name=name)
        elif hasattr(obj, 'get_brief_meta_description'):  # isinstance(obj, AbstractNamed):
            return obj.get_brief_meta_description()
        else:  # tmp
            meta = obj.get_meta(ex=['name', 'caption'])
            return get_str_from_args_kwargs(**meta)

    @classmethod
    def get_meta_chapter_for(
            cls,
            obj,
            level: Optional[int] = DEFAULT_CHAPTER_TITLE_LEVEL,
            name: str = 'Meta',
    ) -> Iterable:
        if level:
            yield cls.build_paragraph(name, level=level)
        if isinstance(obj, BaseInterface) or hasattr(obj, 'get_meta_records'):
            count = len(list(obj.get_meta_records()))
            comment = f'{repr(obj)} has {count} attributes in meta-data:'
            yield cls.build_paragraph(comment)
        yield cls.get_meta_sheet_for(obj, name=f'{name} sheet')

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
            meta_description_placeholders.append('{' + str(formatter) + '}')
        return delimiter.join(meta_description_placeholders)

    @staticmethod
    def _get_column_names(columns: Iterable, ex: Union[str, Sequence, None] = None) -> Generator:
        if ex is None:
            ex = []
        elif isinstance(ex, str):
            ex = [ex]
        for c in columns:
            if c in ex:
                yield EMPTY
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
    @deprecated_with_alternative('SimpleSheet.get_lines()')
    def _get_columnar_lines(
            cls,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            delimiter: str = REPR_DELIMITER,
            max_len: int = DEFAULT_LINE_LEN,
    ) -> Generator:
        count = Auto.acquire(count, DEFAULT_EXAMPLE_COUNT)
        formatter = cls._get_formatter(columns=columns, delimiter=delimiter)
        if with_title:
            column_names = cls._get_column_names(columns)
            title_record = cls._get_cropped_record(column_names, columns=columns, max_len=max_len)
            yield formatter.format(**{k: v.upper() for k, v in title_record.items()})
        for n, r in enumerate(records):
            if count is not None and n >= count:
                break
            r = cls._get_cropped_record(r, columns=columns, max_len=max_len)
            yield formatter.format(**r)

    @classmethod
    def get_sheet_class(cls) -> Optional[Class]:
        return cls._sheet_class

    @classmethod
    def set_sheet_class_inplace(cls, sheet_class: Class):
        cls._sheet_class = sheet_class

    @deprecated_with_alternative('build_sheet().get_lines()')
    def get_encoded_sheet(self, records, columns) -> Iterator[str]:
        sheet = self.build_sheet(records, columns=columns)
        return sheet.get_lines()

    @deprecated_with_alternative('build_paragraph()')
    def get_encoded_paragraph(self, paragraph: Optional[Iterable] = None, level: Optional[int] = None) -> Iterator[str]:
        if isinstance(paragraph, str):
            yield from paragraph.split(PARAGRAPH_CHAR)
        elif isinstance(paragraph, Iterable):
            yield from paragraph
        elif paragraph:
            raise TypeError(paragraph)

    @classmethod
    def _get_display_object(cls, data: Union[str, Iterable]) -> Optional[str]:
        if not data:
            return None
        if hasattr(data, 'get_lines'):  # isinstance(data, DocumentItem)
            data = data.get_lines()
        if isinstance(data, Iterable) and not isinstance(data, str):
            data = PARAGRAPH_CHAR.join(data)
        return data

    @staticmethod
    def _get_display_method() -> Callable:
        return print

    @deprecated_with_alternative('display_item()')
    def display(self, item: Item = AUTO):
        item = Auto.acquire(item, self)
        data = self._get_display_object(item)
        method = self._get_display_method()
        method(data)
        return self

    def __call__(self, obj) -> None:
        return self.display_item(obj)
