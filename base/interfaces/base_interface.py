from abc import ABC, abstractmethod
from typing import Optional, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from base.constants.chars import CROP_SUFFIX
    from base.constants.text import DEFAULT_LINE_LEN
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..constants.chars import CROP_SUFFIX
    from ..constants.text import DEFAULT_LINE_LEN

OptionalFields = Union[str, Iterable, None]


class BaseInterface(ABC):
    @abstractmethod
    def set_props(self, inplace: bool, **kwargs):
        pass

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
    def set_meta(self, inplace: bool = False, **meta):
        pass

    @abstractmethod
    def update_meta(self, **meta):
        pass

    @abstractmethod
    def fill_meta(self, check: bool = True, **meta):
        pass

    @abstractmethod
    def get_compatible_meta(self, other=None, ex: Optional[Iterable] = None, **kwargs) -> dict:
        pass

    @abstractmethod
    def get_meta_items(self, meta: Optional[dict] = None) -> Generator:
        pass

    @abstractmethod
    def get_ordered_meta_names(self, meta: Optional[dict] = None) -> Generator:
        pass

    @abstractmethod
    def get_meta_defaults(self) -> Generator:
        pass

    @abstractmethod
    def get_meta_records(self) -> Generator:
        pass

    @abstractmethod
    def get_str_meta(self) -> str:
        pass

    @abstractmethod
    def get_detailed_repr(self) -> str:
        pass

    @abstractmethod
    def get_one_line_repr(
            self,
            str_meta: Optional[str] = None,
            max_len: int = DEFAULT_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        pass

    @abstractmethod
    def make_new(self, *args, ex: OptionalFields = None, **kwargs):
        pass

    @abstractmethod
    def describe(
            self,
            comment: Optional[str] = None,
            depth: int = 1,
            display=None,
    ):
        pass
