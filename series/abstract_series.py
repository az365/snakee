from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.external import np
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.iterable_mixin import IterableMixin, IterableInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..utils.external import np
    from ..base.abstract.simple_data import SimpleDataWrapper
    from ..base.mixin.iterable_mixin import IterableMixin, IterableInterface

Native = Union[SimpleDataWrapper, IterableMixin, IterableInterface]
Item = Any

if np:  # numpy installed
    Mutable = Union[list, np.ndarray]
    MUTABLE = list, np.ndarray
else:
    Mutable = list
    MUTABLE = list
ARRAY_TYPES = list, tuple
META_MEMBER_MAPPING = dict(_data='values')
DEFAULT_NAME = '-'


class AbstractSeries(IterableMixin, SimpleDataWrapper, ABC):
    def __init__(
            self,
            values: Iterable,
            set_closure: bool = False,
            validate: bool = False,
            name: Optional[str] = None,
    ):
        values = self._get_optional_copy(values, role='values', set_closure=set_closure)
        super().__init__(data=values, name=name or DEFAULT_NAME)
        if validate:
            self.validate()

    @staticmethod
    def _get_optional_copy(array: Iterable, set_closure: bool = False, role: str = 'array') -> Mutable:
        if set_closure:  # data-attributes (keys, values) will be set by closure-link
            if isinstance(array, AbstractSeries) or hasattr(array, 'get_values'):
                array = array.get_values()
            assert isinstance(array, MUTABLE), 'Can set closure for mutable {} only, got {}'.format(role, type(array))
        elif array is None:
            array = list()
        else:  # data-attribute (keys, values) will be set by value copy
            array = list(array)
        return array

    @abstractmethod
    def _get_meta_member_names(self):
        pass

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return META_MEMBER_MAPPING

    def _get_data_member_dict(self) -> dict:
        return {
            self._get_meta_field_by_member_name(f): self.__dict__[f]
            for f in self._get_data_member_names()
        }

    # @deprecated_with_alternative('__class__.__name__')
    def get_class_name(self) -> str:
        return self.__class__.__name__

    def new(self, *args, save_meta: bool = False, **kwargs) -> Native:
        new = self.__class__(*args, **kwargs)
        if save_meta:
            new.set_meta(
                **self.get_meta(),
                inplace=True,
            )
        return new

    def copy(self, validate: bool = False, **kwargs) -> Native:
        return super().copy(validate=validate, **kwargs)

    def get_props(self, ex=None, check: bool = True) -> dict:
        return super().get_props(ex=ex, check=check)

    def get_values(self) -> list:
        return super().get_data()

    @abstractmethod
    def get_items(self) -> list:
        pass

    def set_values(self, values: Iterable, inplace: bool, set_closure: bool = False, validate: bool = False) -> Native:
        values = self._get_optional_copy(values, role='values', set_closure=set_closure)
        return self.set_data(values, reset_dynamic_meta=True, validate=validate, inplace=inplace) or self

    def __iter__(self):
        yield from self.get_items()

    def get_list(self) -> list:
        return list(self.get_items())

    @abstractmethod
    def get_errors(self) -> Iterable:
        pass

    def is_valid(self) -> bool:
        return not list(self.get_errors())

    def validate(self, raise_exception: bool = True, default: Optional[bool] = None) -> Union[Native, bool, None]:
        if self.is_valid():
            return self
        elif raise_exception:
            errors = list(self.get_errors())
            raise ValueError('; '.join(errors))
        else:
            return default
