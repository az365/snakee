from abc import ABC
from typing import Union, Optional, Iterable, Any, NoReturn

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.functions.arguments import get_str_from_annotation, get_str_from_args_kwargs
    from base.interfaces.context_interface import ContextInterface
    from base.interfaces.contextual_interface import ContextualInterface
    from base.interfaces.data_interface import SimpleDataInterface
    from base.abstract.named import AbstractNamed
    from base.abstract.contextual import Contextual
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import AUTO, Auto
    from ..functions.arguments import get_str_from_annotation, get_str_from_args_kwargs
    from ..interfaces.context_interface import ContextInterface
    from ..interfaces.contextual_interface import ContextualInterface
    from ..interfaces.data_interface import SimpleDataInterface
    from .named import AbstractNamed
    from .contextual import Contextual

Native = SimpleDataInterface
Data = Union[Iterable, Any]
OptionalFields = Optional[Union[str, Iterable]]
Source = Optional[ContextualInterface]
Context = Optional[ContextInterface]

DATA_MEMBER_NAMES = ('_data', )
DYNAMIC_META_FIELDS = tuple()


class SimpleDataWrapper(AbstractNamed, SimpleDataInterface, ABC):
    def __init__(
            self, data, name: str,
    ):
        self._data = data
        super().__init__(name=name)

    @classmethod
    def _get_data_member_names(cls):
        return DATA_MEMBER_NAMES

    def get_data(self) -> Data:
        return self._data

    def set_data(self, data: Data, inplace: bool, reset_dynamic_meta: bool = True, **kwargs) -> Native:
        if inplace:
            self._data = data
            if reset_dynamic_meta:
                meta = self.get_static_meta()
            else:
                meta = dict()
            meta.update(kwargs)
            if meta:
                self.set_meta(**meta, inplace=True)
            return self
        else:
            if reset_dynamic_meta:
                meta = self.get_static_meta()
            else:
                meta = self.get_meta()
            meta.update(kwargs)
            try:
                return self.__class__(data, **meta)
            except TypeError as e:  # __init__() got an unexpected keyword argument '...'
                self._raise_init_error(e, type(data), reset_dynamic_meta=reset_dynamic_meta, inplace=inplace, **meta)

    def _raise_init_error(self, msg: str, *args, **kwargs) -> NoReturn:
        class_name = self.__class__.__name__
        annotations = get_str_from_annotation(self.__class__)
        ann_str = '\n(available args are: {})'.format(annotations) if annotations else ''
        arg_str = get_str_from_args_kwargs(*args, **kwargs)
        raise TypeError('{}: {}({}) {}'.format(msg, class_name, arg_str, ann_str))

    def apply_to_data(self, function, *args, dynamic=False, inplace: bool = False, **kwargs) -> Native:
        data = function(self.get_data(), *args, **kwargs)
        return self.set_data(data, inplace=inplace, reset_dynamic_meta=dynamic)

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
