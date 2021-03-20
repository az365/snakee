from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg

OptionalFields = Optional[Union[str, Iterable]]


class BaseInterface(ABC):
    @abstractmethod
    def set_inplace(self, **kwargs):
        pass

    @abstractmethod
    def set_outplace(self, **kwargs):
        pass

    @abstractmethod
    def get_key_member_values(self) -> list:
        pass

    @classmethod
    @abstractmethod
    def get_meta_fields_list(cls) -> list:
        pass

    @abstractmethod
    def get_props(self, ex: OptionalFields = None, check: bool = True) -> dict:
        pass

    @abstractmethod
    def get_meta(self, ex: OptionalFields = None) -> dict:
        pass

    @abstractmethod
    def set_meta(self, inplace=False, **meta):
        pass

    @abstractmethod
    def update_meta(self, **meta):
        pass

    @abstractmethod
    def fill_meta(self, check=True, **meta):
        pass

    @abstractmethod
    def get_compatible_meta(self, other=arg.DEFAULT, ex=None, **kwargs) -> dict:
        pass

    @abstractmethod
    def get_str_meta(self):
        pass
