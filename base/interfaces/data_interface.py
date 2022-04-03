from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoCount, OptionalFields
    from base.constants.chars import CROP_SUFFIX, DEFAULT_LINE_LEN
    from base.interfaces.base_interface import BaseInterface, AutoOutput
    from base.interfaces.line_output_interface import LineOutputInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoCount, OptionalFields
    from ..constants.chars import CROP_SUFFIX, DEFAULT_LINE_LEN
    from .base_interface import BaseInterface, AutoOutput
    from .line_output_interface import LineOutputInterface

Data = Union[Iterable, Any]


class SimpleDataInterface(BaseInterface, ABC):
    @abstractmethod
    def get_name(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_caption(self) -> str:
        pass

    @abstractmethod
    def get_data(self) -> Data:
        pass

    @abstractmethod
    def set_data(self, data: Data, inplace: bool):
        pass

    @abstractmethod
    def apply_to_data(self, function: Callable, *args, dynamic: bool = False, **kwargs):
        pass

    @abstractmethod
    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        pass

    @abstractmethod
    def get_count_repr(self, default: str = '<iter>') -> str:
        pass

    def get_one_line_repr(
            self,
            str_meta: Union[str, Auto, None] = AUTO,
            max_len: int = DEFAULT_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        pass

    @abstractmethod
    def describe(
            self,
            show_header: bool = True,
            count: AutoCount = AUTO,
            comment: Optional[str] = None,
            depth: int = 1,
            output: AutoOutput = AUTO,
            as_dataframe: bool = Auto,
            **kwargs
    ):
        pass
