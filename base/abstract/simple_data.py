from abc import ABC
from typing import Optional, Iterable, Generator, Union, Any, NoReturn

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Count
    from base.constants.chars import EMPTY, ITEMS_DELIMITER, REPR_DELIMITER, SMALL_INDENT, PARAGRAPH_CHAR
    from base.functions.arguments import get_str_from_annotation, get_str_from_args_kwargs
    from base.interfaces.data_interface import SimpleDataInterface
    from base.mixin.display_mixin import DEFAULT_EXAMPLE_COUNT
    from base.mixin.data_mixin import DataMixin, UNK_COUNT_STUB, DEFAULT_CHAPTER_TITLE_LEVEL
    from base.abstract.named import AbstractNamed, DisplayInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Count
    from ..constants.chars import EMPTY, ITEMS_DELIMITER, REPR_DELIMITER, SMALL_INDENT, PARAGRAPH_CHAR
    from ..functions.arguments import get_str_from_annotation, get_str_from_args_kwargs
    from ..interfaces.data_interface import SimpleDataInterface
    from ..mixin.display_mixin import DEFAULT_EXAMPLE_COUNT
    from ..mixin.data_mixin import DataMixin, UNK_COUNT_STUB, DEFAULT_CHAPTER_TITLE_LEVEL
    from .named import AbstractNamed, DisplayInterface

Native = SimpleDataInterface
Data = Any
OptionalFields = Optional[Union[str, Iterable]]

DATA_MEMBER_NAMES = '_data',
DYNAMIC_META_FIELDS = tuple()

MAX_OUTPUT_ROW_COUNT, MAX_DATAFRAME_ROW_COUNT = 200, 20
MAX_BRIEF_REPR_LEN = 30
INCORRECT_COUNT = -1


class SimpleDataWrapper(AbstractNamed, DataMixin, SimpleDataInterface, ABC):
    def __init__(self, data, name: str, caption: str = EMPTY):
        self._data = data
        super().__init__(name=name, caption=caption)

    @classmethod
    def _get_data_member_names(cls):
        return DATA_MEMBER_NAMES

    def get_data(self) -> Data:
        return self._data

    def set_data(self, data: Data, inplace: bool, reset_dynamic_meta: bool = True, safe=True, **kwargs) -> Native:
        if inplace:
            self._set_data_inplace(data)
            if reset_dynamic_meta:
                meta = self.get_static_meta()
            else:
                meta = dict()
            meta.update(kwargs)
            if meta:
                self.set_meta(**meta, safe=safe, inplace=True)
            return self
        else:
            if reset_dynamic_meta:
                meta = self.get_static_meta()
            else:
                meta = self.get_meta()
            meta.update(kwargs)
            if safe:
                meta = self._get_safe_meta(**meta)
            try:
                return self.__class__(data, **meta)
            except TypeError as e:  # __init__() got an unexpected keyword argument '...'
                self._raise_init_error(e, type(data), reset_dynamic_meta=reset_dynamic_meta, inplace=inplace, **meta)

    def _set_data_inplace(self, data: Data) -> Native:
        self._data = data
        return self

    def _raise_init_error(self, msg: str, *args, **kwargs) -> NoReturn:
        class_name = self.__class__.__name__
        annotations = get_str_from_annotation(self.__class__)
        ann_str = f'{PARAGRAPH_CHAR}(available args are: {annotations})' if annotations else EMPTY
        arg_str = get_str_from_args_kwargs(*args, **kwargs)
        raise TypeError(f'{msg}: {class_name}({arg_str}) {ann_str}')

    def apply_to_data(self, function, *args, dynamic=False, inplace: bool = False, **kwargs) -> Native:
        data = function(self.get_data(), *args, **kwargs)
        return self.set_data(data, inplace=inplace, reset_dynamic_meta=dynamic)

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS  # empty (no meta fields)

    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        meta = self.get_meta(ex=ex)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f, None)
        return meta

    def get_compatible_static_meta(self, other: Optional[Native] = None, ex=None, **kwargs) -> dict:
        meta = self.get_compatible_meta(other=other, ex=ex, **kwargs)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f, None)
        return meta

    def get_str_count(self, default: Optional[str] = UNK_COUNT_STUB) -> Optional[str]:
        if hasattr(self, 'get_count'):
            count = self.get_count()
        else:
            count = None
        if count is not None:
            return str(count)
        else:
            return default

    def get_count_repr(self, default: str = UNK_COUNT_STUB) -> str:
        count = self.get_str_count(default=default)
        if count is None:
            count = default
        return f'{count} items'

    def get_shape_repr(self) -> str:
        len_repr = self.get_count_repr()
        if hasattr(self, 'get_column_repr'):
            column_repr = self.get_column_repr()
        else:
            column_repr = None
        dimensions_repr = list()
        if len_repr:
            dimensions_repr.append(len_repr)
        if column_repr:
            dimensions_repr.append(column_repr)
        return ITEMS_DELIMITER.join(dimensions_repr)

    def get_str_headers(self) -> Generator:
        yield self.get_one_line_repr()

    def get_str_title(self) -> str:
        obj_name = self.get_name()
        class_name = self.__class__.__name__
        if obj_name:
            title = f'{obj_name} {class_name}'
        else:
            title = f'Unnamed {class_name}'
        return title

    def get_brief_repr(self) -> str:
        try:
            repr_line = repr(self)
        except RecursionError:
            repr_line = None
        if repr_line is None or len(repr_line) > MAX_BRIEF_REPR_LEN:
            if self.get_name():
                repr_line = super().get_brief_repr()  # AbstractNamed.get_brief_repr()
            else:
                repr_line = f'{self.__class__.__name__}(...)'
        return repr_line

    def get_count(self) -> int:
        if self.has_data():
            data = self.get_data()
            if isinstance(data, SimpleDataWrapper) or hasattr(data, 'get_count'):
                return data.get_count()
            try:
                return len(data)
            except TypeError:  # NoneType object has no len-attribute
                return INCORRECT_COUNT
        else:
            return 0

    def has_data(self) -> bool:
        return bool(self.get_data())

    def get_data_sheet(self, count: int = DEFAULT_EXAMPLE_COUNT, name: Optional[str] = 'Data sheet'):
        display = self.get_display()
        sheet_class = display.get_sheet_class()
        data = self.get_data()
        if isinstance(data, dict):
            data_sheet = sheet_class.from_one_record(data, name=name)
        elif isinstance(data, Iterable):
            data_sheet = sheet_class.from_items(data, name=name)
        elif isinstance(data, SimpleDataWrapper) or hasattr(data, 'get_data_sheet'):
            data_sheet = data.get_data_sheet()
        else:
            data_sheet = sheet_class.from_items([data], name=name)
        return data_sheet

    def get_data_chapter(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            title: Optional[str] = 'Data',
            comment: Optional[str] = None,
    ) -> Generator:
        display = self.get_display()
        yield display.build_paragraph(title, level=DEFAULT_CHAPTER_TITLE_LEVEL, name=f'{title} title')
        if comment:
            yield display.build_paragraph(comment, name=f'{title} comment')
        if hasattr(self, 'get_data_caption'):
            yield self.get_data_caption()
        if self.has_data():
            shape_repr = self.get_shape_repr()
            if shape_repr and count is not None:
                yield f'First {count} data items from {shape_repr}:'
            yield self.get_data_sheet(count=count, name=f'{title} sheet')
        else:
            yield '(data attribute is empty)'

    def get_description_items(
            self,
            comment: Optional[str] = None,
            depth: int = 1,
            count: Count = None,
    ) -> Generator:
        display = self.get_display()
        yield display.get_header_chapter_for(self, comment=comment)
        if depth > 0:
            yield display.get_meta_chapter_for(self)
        if self.has_data():
            yield self.get_data_chapter(count=count)
        yield from self.get_child_description_items(depth)
