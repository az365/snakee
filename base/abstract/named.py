from abc import ABC
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto
    from base.constants.chars import EMPTY
    from base.mixin.line_output_mixin import LineOutputMixin
    from base.abstract.abstract_base import AbstractBaseObject
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import Auto
    from ..constants.chars import EMPTY
    from ..mixin.line_output_mixin import LineOutputMixin
    from .abstract_base import AbstractBaseObject

Native = Union[AbstractBaseObject, LineOutputMixin]

SPECIFIC_MEMBERS = ('_name', )


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
        named = self.update_meta(caption=caption, inplace=inplace)
        return self._assume_native(named)

    def is_defined(self) -> bool:
        return Auto.is_defined(self.get_name())

    @classmethod
    def _get_key_member_names(cls) -> list:
        return super()._get_key_member_names() + list(SPECIFIC_MEMBERS)

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return cls._get_key_member_names()

    def get_brief_repr(self) -> str:
        return "{}('{}')".format(self.__class__.__name__, self.get_name())

    def __repr__(self):
        return self.get_brief_repr()

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
