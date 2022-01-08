from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from base.abstract.simple_data import SimpleDataWrapper
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base.abstract.simple_data import SimpleDataWrapper

Native = SimpleDataWrapper
Item = Any

ARRAY_TYPES = list, tuple
META_MEMBER_MAPPING = dict(_data='values')
DEFAULT_NAME = '-'


class AbstractSeries(SimpleDataWrapper, ABC):
    def __init__(
            self,
            values: Iterable,
            validate: bool = False,
            name: Optional[str] = None,
    ):
        super().__init__(
            data=list(values),
            name=name or DEFAULT_NAME,
        )
        if validate:
            self.validate()

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

    def copy(self):
        return self.__class__(
            validate=False,
            **self.get_props()
        )

    def get_props(self, ex=None, check: bool = True) -> dict:
        return super().get_props(ex=ex, check=check)

    def get_values(self) -> list:
        return super().get_data()

    @abstractmethod
    def get_items(self) -> list:
        pass

    def set_values(self, values: Iterable, inplace: bool):
        if not isinstance(values, ARRAY_TYPES):
            values = list(values)
        if inplace:
            self.set_data(values, inplace=True)
        else:
            new = self.copy()
            new.set_data(values, inplace=True)
            return new

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
