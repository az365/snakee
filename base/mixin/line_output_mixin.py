from abc import ABC
from typing import Optional, Iterable, Generator, Sequence, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoCount, Class
    from base.classes.display import DefaultDisplay, PREFIX_FIELD, DEFAULT_ROWS_COUNT
    from base.functions.arguments import get_name, get_value
    from base.constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from base.interfaces.line_output_interface import LineOutputInterface, AutoOutput, LoggingLevel
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoCount, Class
    from ..classes.display import DefaultDisplay, PREFIX_FIELD, DEFAULT_ROWS_COUNT
    from ..functions.arguments import get_name, get_value
    from ..constants.chars import DEFAULT_LINE_LEN, REPR_DELIMITER, SMALL_INDENT, EMPTY
    from ..interfaces.line_output_interface import LineOutputInterface, AutoOutput, LoggingLevel

_display = DefaultDisplay()


class LineOutputMixin(LineOutputInterface, ABC):
    @classmethod
    def get_display(cls, display: Optional[DefaultDisplay] = None) -> DefaultDisplay:
        if isinstance(display, DefaultDisplay) or hasattr(display, 'display_paragraph'):
            return display
        else:
            global _display
            return _display

    @classmethod
    def set_display(cls, display: DefaultDisplay):
        cls.set_display_inplace(display)
        return cls

    @classmethod
    def set_display_inplace(cls, display: DefaultDisplay):
        global _display
        _display = display

    display = property(get_display, set_display_inplace)

    # @deprecated
    @classmethod
    def get_output(cls, output: AutoOutput = AUTO) -> Optional[Class]:
        return cls.get_display().get_output(output)

    # @deprecated
    def output_line(self, line: str, output: AutoOutput = AUTO) -> None:
        return self.get_display().output_line(line, output=output)

    # @deprecated
    def output_blank_line(self, output: AutoOutput = AUTO) -> None:
        self.output_line(EMPTY, output=output)

    # @deprecated
    @classmethod
    def _get_formatter(cls, columns: Sequence, delimiter: str = REPR_DELIMITER) -> str:
        return cls.get_display()._get_formatter(columns, delimiter=delimiter)

    # @deprecated
    @classmethod
    def _get_column_names(cls, columns: Iterable, ex: Union[str, Sequence, None] = None) -> Generator:
        return cls.get_display()._get_column_names(columns, ex=ex)

    # @deprecated
    @classmethod
    def _get_column_lens(cls, columns: Iterable, max_len: Optional[int] = None) -> Generator:
        return cls.get_display()._get_column_lens(columns, max_len=max_len)

    # @deprecated
    @classmethod
    def _get_cropped_record(
            cls,
            item: Union[dict, Iterable],
            columns: Sequence,
            max_len: int = DEFAULT_LINE_LEN,
            ex: Union[str, Sequence, None] = None,
    ) -> dict:
        return cls.get_display()._get_cropped_record(item, columns=columns, max_len=max_len, ex=ex)

    # @deprecated
    @classmethod
    def _get_columnar_lines(
            cls,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            prefix: str = SMALL_INDENT,
            delimiter: str = REPR_DELIMITER,
            max_len: int = DEFAULT_LINE_LEN,
    ) -> Generator:
        return cls.get_display()._get_columnar_lines(records, columns, count, with_title, prefix, delimiter, max_len)

    # @deprecated
    def display_sheet(
            self,
            records: Iterable,
            columns: Sequence,
            count: AutoCount = None,
            with_title: bool = True,
            output: AutoOutput = AUTO,
    ) -> None:
        return self.get_display().display_sheet(records, columns, count=count, with_title=with_title, output=output)

    # @deprecated
    def display_paragraph(self, paragraph: Iterable, level: Optional[int] = None, output=AUTO) -> None:
        return self.get_display().display_paragraph(paragraph, level=level, output=output)

    # @deprecated
    def display_item(self, item, item_type='line', output=AUTO, **kwargs) -> None:
        return self.get_display().display_item(item, item_type=item_type, output=output, **kwargs)
