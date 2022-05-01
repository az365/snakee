from abc import ABC
from typing import Optional, Iterable, Sequence, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoCount, Class
    from base.classes.display import DefaultDisplay, PREFIX_FIELD, DEFAULT_ROWS_COUNT
    from base.functions.arguments import get_name, get_value
    from base.constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from base.interfaces.display_interface import DisplayInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoCount, Class
    from ..classes.display import DefaultDisplay, PREFIX_FIELD, DEFAULT_ROWS_COUNT
    from ..functions.arguments import get_name, get_value
    from ..constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from ..interfaces.display_interface import DisplayInterface

AutoOutput = Union[Auto, DisplayInterface]

_display = DefaultDisplay()


class LineOutputMixin(DisplayInterface, ABC):
    def get_display(self, display: Optional[DefaultDisplay] = None) -> DefaultDisplay:
        if isinstance(display, DisplayInterface) or hasattr(display, 'display_item'):
            return display
        elif Auto.is_defined(display):
            raise TypeError(display)
        else:
            global _display
            return _display

    @classmethod
    def set_display(cls, display: DefaultDisplay):
        cls.set_display_inplace(display)
        return cls

    def set_display_inplace(self, display: DefaultDisplay):
        global _display
        _display = display

    display = property(get_display, set_display_inplace)

    # @deprecated
    def get_output(self, output: AutoOutput = AUTO) -> Optional[Class]:
        return self.get_display(output)

    # @deprecated
    def output_blank_line(self, output=AUTO) -> None:
        self.output_line(EMPTY, output=output)

    # @deprecated
    def output_line(self, line: str, output=AUTO) -> None:
        return self.get_display(output).add_to_paragraph(line)

    def add_to_paragraph(self, line: str) -> None:
        return self.get_display().add_to_paragraph(line)

    # @deprecated
    def display_paragraph(self, paragraph: Iterable, level: Optional[int] = None, output=AUTO) -> None:
        return self.get_display(output).display_paragraph(paragraph, level=level)

    # @deprecated
    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            output: AutoOutput = AUTO,
    ) -> None:
        return self.get_display(output).display_sheet(records, columns, count=count, with_title=with_title)

    # @deprecated
    def display_item(self, item, item_type='line', output=AUTO, **kwargs) -> None:
        return self.get_display(output).display_item(item, item_type=item_type, **kwargs)
