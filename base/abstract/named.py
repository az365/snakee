from abc import ABC
from typing import Optional, Generator, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.constants.chars import EMPTY, PY_INDENT, REPR_DELIMITER, DEFAULT_LINE_LEN
    from base.functions.arguments import get_str_from_args_kwargs
    from base.mixin.line_output_mixin import LineOutputMixin, AutoOutput
    from base.abstract.abstract_base import AbstractBaseObject
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import AUTO, Auto
    from ..constants.chars import EMPTY, PY_INDENT, REPR_DELIMITER, DEFAULT_LINE_LEN
    from ..functions.arguments import get_str_from_args_kwargs
    from ..mixin.line_output_mixin import LineOutputMixin, AutoOutput
    from .abstract_base import AbstractBaseObject

Native = Union[AbstractBaseObject, LineOutputMixin]

SPECIFIC_MEMBERS = ('_name', )
BRIEF_META_ROW_FORMATTER = '{prefix}{key:10} {value}'


class AbstractNamed(AbstractBaseObject, LineOutputMixin, ABC):
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
        return Auto.is_defined(self.get_name())

    @classmethod
    def _get_key_member_names(cls) -> list:
        return super()._get_key_member_names() + list(SPECIFIC_MEMBERS)

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return cls._get_key_member_names()

    def get_brief_meta_description(self, prefix: str = PY_INDENT) -> Generator:
        yield BRIEF_META_ROW_FORMATTER.format(prefix=prefix, key='name:', value=self.get_name())
        yield BRIEF_META_ROW_FORMATTER.format(prefix=prefix, key='caption:', value=self.get_caption())
        meta = self.get_meta(ex=['name', 'caption'])
        if meta:
            line = BRIEF_META_ROW_FORMATTER.format(prefix=prefix, key='meta:', value=get_str_from_args_kwargs(**meta))
            yield line[:DEFAULT_LINE_LEN]

    def get_meta_description(self, prefix: str = PY_INDENT, delimiter: str = REPR_DELIMITER) -> Generator:
        yield from self.get_brief_meta_description(prefix=prefix)

    def get_brief_repr(self) -> str:
        return "{cls}('{name}')".format(cls=self.__class__.__name__, name=self.get_name())

    def __repr__(self):
        return self.get_brief_repr()

    def get_str_headers(self) -> Generator:
        yield self.get_brief_repr()

    def describe(
            self,
            show_header: bool = True,
            comment: Optional[str] = None,
            depth: int = 1,
            output: AutoOutput = AUTO,  # deprecated
    ):
        display = self.get_display(output)
        if show_header:
            display.display_paragraph(self.get_name(), level=1)
            for line in self.get_str_headers():
                display.output_line(line)
        display.output_line(comment)
        display.output_line(self.get_meta_description())
        if depth > 0:
            for attribute, value in self.get_meta_items():
                if isinstance(value, AbstractBaseObject) or hasattr(value, 'describe'):
                    display.display_paragraph(attribute, level=3)
                    value.describe(show_header=False, depth=depth - 1, output=output)
        display.display_paragraph()

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
