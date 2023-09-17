from abc import ABC
from typing import Optional, Iterable, Sequence

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from base.classes.typing import Count, Class
    from base.classes.display import DefaultDisplay, DEFAULT_EXAMPLE_COUNT
    from base.functions.arguments import get_value
    from base.constants.chars import REPR_DELIMITER, SMALL_INDENT, EMPTY
    from base.interfaces.display_interface import DisplayInterface, Style
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ..classes.typing import Count, Class
    from ..classes.display import DefaultDisplay, DEFAULT_EXAMPLE_COUNT
    from ..functions.arguments import get_value
    from ..constants.chars import REPR_DELIMITER, SMALL_INDENT, EMPTY
    from ..interfaces.display_interface import DisplayInterface, Style


class DisplayMixin(DisplayInterface, ABC):
    _display_obj: DisplayInterface = DefaultDisplay()

    @classmethod
    def get_display(cls, display: Optional[DisplayInterface] = None) -> DisplayInterface:
        if isinstance(display, (DefaultDisplay, DisplayInterface)) or hasattr(display, 'display_item'):
            return display
        elif display:
            raise TypeError(display)
        else:
            return cls._display_obj

    @classmethod
    def set_display(cls, display: DisplayInterface) -> Class:
        cls.set_display_inplace(display)
        return cls

    def _set_display_inplace(self, display: DisplayInterface):
        self.set_display_inplace(display)

    def _get_display(self):
        return self._display_obj

    @classmethod
    def set_display_inplace(cls, display: DisplayInterface):
        cls._display_obj = display

    display = property(_get_display, _set_display_inplace)

    @classmethod
    def get_sheet_class(cls) -> Optional[Class]:
        return cls.get_display().get_sheet_class()

    @classmethod
    def set_sheet_class_inplace(cls, sheet_class: Class):
        cls.get_display().set_sheet_class_inplace(sheet_class)

    @deprecated_with_alternative('get_display().display_paragraph()')
    def display_paragraph(
            self,
            paragraph: Optional[Iterable] = None,
            level: Optional[int] = None,
            style: Style = None,
    ) -> None:
        return self.get_display().display_paragraph(paragraph, level=level, style=style)

    @deprecated_with_alternative('get_display().display_sheet()')
    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: Count = None,
            with_title: bool = True,
            style: Style = None,
    ) -> None:
        return self.get_display().display_sheet(records, columns, count=count, with_title=with_title, style=style)

    @deprecated_with_alternative('get_display().display_item()')
    def display_item(self, item, item_type='paragraph', output=None, **kwargs) -> None:
        return self.get_display(output).display_item(item, item_type=item_type, **kwargs)
