from abc import ABC
from typing import Union, Optional, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.data_interface import DataInterface
    from base.abstract.abstract_base import AbstractBaseObject
    from base.abstract.contextual import Contextual
    from base.interfaces.contextual_interface import ContextualInterface
    from base.interfaces.context_interface import ContextInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from utils import arguments as arg
    from base.interfaces.data_interface import DataInterface
    from .abstract_base import AbstractBaseObject
    from .contextual import ContextualInterface, Contextual
    from base.interfaces.context_interface import ContextInterface

Data = Union[Iterable, Any]
OptionalFields = Optional[Union[str, Iterable]]
Source = Optional[ContextualInterface]
Context = Optional[ContextInterface]

DATA_MEMBER_NAMES = ('_data', )
DYNAMIC_META_FIELDS = tuple()


class DataWrapper(Contextual, DataInterface, ABC):
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

    def get_data(self) -> Data:
        return self._data

    def set_data(self, data: Data, inplace: bool):
        if inplace:
            self._data = data
            self.set_meta(**self.get_static_meta())
        else:
            return DataWrapper(data, **self.get_static_meta())

    def apply_to_data(self, function, *args, dynamic=False, **kwargs):
        return self.__class__(
            data=function(self.get_data(), *args, **kwargs),
            **self.get_static_meta() if dynamic else self.get_meta()
        )

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        meta = self.get_meta(ex=ex)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f)
        return meta
