from abc import ABC, abstractmethod
from typing import Optional, Iterable, Sequence, Any

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

    # @deprecated
    @abstractmethod
    def display_paragraph(
            self,
            paragraph: Optional[Iterable] = None,
            level: Optional[int] = None,
            style=None,
    ) -> None:
        pass

    # @deprecated
    @abstractmethod
    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: Optional[int] = None,
            with_title: bool = True,
            style=None,
            name: str = '',
    ) -> None:
        pass

    @classmethod
    @abstractmethod
    def get_sheet_class(cls) -> Optional[Class]:
        pass

    @classmethod
    @abstractmethod
    def set_sheet_class_inplace(cls, sheet_class: Class):
        pass
