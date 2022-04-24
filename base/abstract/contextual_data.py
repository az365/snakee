from typing import Union, Optional, Iterable, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.interfaces.context_interface import ContextInterface
    from base.mixin.data_mixin import DataMixin
    from base.mixin.contextual_mixin import ContextualMixin
    from base.abstract.abstract_base import AbstractBaseObject
    from base.abstract.sourced import Sourced, SourcedInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import AUTO, Auto
    from ..interfaces.context_interface import ContextInterface
    from ..mixin.data_mixin import DataMixin
    from ..mixin.contextual_mixin import ContextualMixin
    from .abstract_base import AbstractBaseObject
    from .sourced import Sourced, SourcedInterface

Native = Union[Sourced, ContextualMixin, DataMixin]
Data = Any
OptionalFields = Union[str, Iterable, None]
Source = Optional[SourcedInterface]
Context = Optional[ContextInterface]

DATA_MEMBER_NAMES = ('_data', )
DYNAMIC_META_FIELDS = tuple()


class ContextualDataWrapper(Sourced, ContextualMixin, DataMixin):
    def __init__(
            self,
            data: Data,
            name: str,
            caption: str = '',
            source: Source = None,
            context: Context = None,
            check: bool = True,
    ):
        self._data = data
        super().__init__(name=name, caption=caption, source=source, check=check)
        if Auto.is_defined(context):
            self.set_context(context, reset=not check, inplace=True)

    @classmethod
    def _get_data_member_names(cls):
        return DATA_MEMBER_NAMES

    def get_data(self) -> Data:
        return self._data

    def set_data(self, data: Data, inplace: bool, **kwargs) -> Native:
        if inplace:
            self._data = data
            return self.set_meta(**self.get_static_meta())
        else:
            return self.__class__(data, **self.get_static_meta())

    def apply_to_data(self, function, *args, dynamic=False, **kwargs):
        return self.__class__(
            data=function(self._get_data(), *args, **kwargs),
            **self.get_static_meta() if dynamic else self.get_meta()
        )

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        meta = self.get_meta(ex=ex)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f, None)
        return meta

    def get_compatible_static_meta(self, other=AUTO, ex=None, **kwargs) -> dict:
        meta = self.get_compatible_meta(other=other, ex=ex, **kwargs)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f, None)
        return meta

    def get_str_count(self, default: str = '(iter)') -> str:
        if hasattr(self, 'get_count'):
            count = self.get_count()
        else:
            count = None
        if Auto.is_defined(count):
            return str(count)
        else:
            return default

    def get_count_repr(self, default: str = '<iter>') -> str:
        count = self.get_str_count()
        if not Auto.is_defined(count):
            count = default
        return '{} items'.format(count)
