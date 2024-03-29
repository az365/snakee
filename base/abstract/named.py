from abc import ABC
from typing import Optional, Iterator, Generator, Union

try:  # Assume we're a submodule in a package.
    from base.constants.chars import EMPTY, TAB_INDENT, REPR_DELIMITER
    from base.functions.arguments import get_str_from_args_kwargs, get_cropped_text
    from base.interfaces.display_interface import DisplayInterface
    from base.mixin.display_mixin import DisplayMixin
    from base.abstract.abstract_base import AbstractBaseObject
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..constants.chars import EMPTY, TAB_INDENT, REPR_DELIMITER
    from ..functions.arguments import get_str_from_args_kwargs, get_cropped_text
    from ..interfaces.display_interface import DisplayInterface
    from ..mixin.display_mixin import DisplayMixin
    from .abstract_base import AbstractBaseObject

Native = Union[AbstractBaseObject, DisplayMixin]

SPECIFIC_MEMBERS = '_name',
BRIEF_META_ROW_FORMATTER = '{prefix}{key:10} {value}'


class AbstractNamed(AbstractBaseObject, DisplayMixin, ABC):
    def __init__(self, name: str, caption: Optional[str] = EMPTY):
        super().__init__()
        self._name = name
        self._caption = caption

    def get_name(self) -> str:
        return self._name

    def set_name(self, name: str, inplace: bool = True) -> Native:
        if inplace:
            self._name = name
            return self
        else:
            props = self.get_props(ex='name')
            if props:
                return self.__class__(name=name, **props)
            else:
                return self.__class__(name=name)

    def get_caption(self) -> str:
        return self._caption

    def set_caption(self, caption: str, inplace: bool = True) -> Native:
        named = self.set_props(caption=caption, inplace=inplace)
        return self._assume_native(named)

    def is_defined(self) -> bool:
        return self.get_name() is not None

    @classmethod
    def _get_key_member_names(cls) -> list:
        return super()._get_key_member_names() + list(SPECIFIC_MEMBERS)

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return cls._get_key_member_names()

    def get_brief_meta_description(self, prefix: str = TAB_INDENT) -> Iterator[str]:
        yield BRIEF_META_ROW_FORMATTER.format(prefix=prefix, key='name:', value=self.get_name())
        yield BRIEF_META_ROW_FORMATTER.format(prefix=prefix, key='caption:', value=self.get_caption())
        meta = self.get_meta(ex=['name', 'caption'])
        if meta:
            line = BRIEF_META_ROW_FORMATTER.format(prefix=prefix, key='meta:', value=get_str_from_args_kwargs(**meta))
            yield get_cropped_text(line)

    def get_brief_repr(self) -> str:
        cls_name = self.__class__.__name__
        obj_name = self.get_name()
        return f'{cls_name}({repr(obj_name)})'

    def __repr__(self):
        return self.get_brief_repr()

    def get_str_headers(self) -> Iterator[str]:
        yield self.get_brief_repr()

    def get_child_description_items(self, depth: int = 2) -> Generator:
        if depth > 1:
            for attribute, value in self.get_meta_items():
                if isinstance(value, AbstractBaseObject) or hasattr(value, 'get_description_items'):
                    yield value.get_description_items(depth=depth - 1)

    def get_description_items(self, comment: Optional[str] = None, depth: int = 2) -> Generator:
        display = self.get_display()
        yield display.get_header_chapter_for(self, comment=comment)
        if depth > 0:
            yield display.get_meta_chapter_for(self)
        yield from self.get_child_description_items(depth)

    def describe(
            self,
            comment: Optional[str] = None,
            depth: int = 2,
            display: Optional[DisplayInterface] = None,
    ) -> Native:
        display = self.get_display(display)
        for i in self.get_description_items(comment=comment, depth=depth):
            display.display_item(i)
        return self

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
