from abc import ABC, abstractmethod
from typing import Optional, Iterable, Sequence, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoCount, Class
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoCount, Class

Display = Class
AutoDisplay = Union[Auto, Display]
AutoStyle = Union[Auto, str]


class DisplayInterface(ABC):
    @abstractmethod
    def get_display(self, display: AutoDisplay = AUTO) -> Display:
        pass

    @abstractmethod
    def display(self, obj=AUTO) -> None:
        pass

    @abstractmethod
    def append(self, text: str) -> None:
        pass

    @abstractmethod
    def display_paragraph(
            self,
            paragraph: Optional[Iterable] = None,
            level: Optional[int] = None,
            style: AutoStyle = AUTO,
    ) -> None:
        pass

    @abstractmethod
    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            style: AutoStyle = AUTO,
    ) -> None:
        pass

    @abstractmethod
    def display_item(self, item, item_type='line', **kwargs) -> None:
        pass
