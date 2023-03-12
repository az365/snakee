from abc import ABC, abstractmethod
from typing import Optional, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Class
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Class

Item = Any
Style = Optional[str]

DEFAULT_EXAMPLE_COUNT = 10


class DisplayInterface(ABC):
    @abstractmethod
    def get_display(self, display=None):
        pass

    @abstractmethod
    def display(self, item: Item = None) -> None:
        pass

    @abstractmethod
    def display_item(self, item, item_type='paragraph', **kwargs) -> None:
        pass

    @classmethod
    @abstractmethod
    def get_sheet_class(cls) -> Optional[Class]:
        pass

    @classmethod
    @abstractmethod
    def set_sheet_class_inplace(cls, sheet_class: Class):
        pass
