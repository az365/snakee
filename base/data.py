from abc import ABC
from typing import Union, Optional, Iterable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract_base import AbstractSnakeeBaseObject
    from base.contextual import ContextualInterface, Contextual
    from base.context_interface import ContextInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .abstract_base import AbstractSnakeeBaseObject
    from .contextual import ContextualInterface, Contextual
    from .context_interface import ContextInterface


OptionalFields = Optional[Union[str, Iterable]]
Source = Optional[ContextualInterface]
Context = Optional[ContextInterface]

DATA_MEMBER_NAMES = ('_data', )
DYNAMIC_MEMBER_NAMES = tuple()


class DataWrapper(Contextual, ABC):
    def __init__(
            self, data, name: str,
            source: Source = None,
            context: Context = None,
            check: bool = True,
    ):
        self._data = data
        super().__init__(name=name, source=source, context=context, check=check)

    @classmethod
    def _get_data_member_names(cls):
        return DATA_MEMBER_NAMES

    def get_data(self):
        return self._data

    def apply_to_data(self, function, *args, dynamic=False, **kwargs):
        return self.__class__(
            data=function(self.get_data(), *args, **kwargs),
            **self.get_static_meta() if dynamic else self.get_meta()
        )

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_MEMBER_NAMES

    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        meta = self.get_meta(ex=ex)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f)
        return meta
