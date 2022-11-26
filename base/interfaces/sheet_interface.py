from abc import ABC, abstractmethod
from typing import Optional, Iterable, Iterator, Tuple, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Name, Count
    from base.constants.chars import EMPTY, REPR_DELIMITER, DEFAULT_LINE_LEN
    from base.interfaces.iterable_interface import IterableInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Name, Count
    from ..constants.chars import EMPTY, REPR_DELIMITER, DEFAULT_LINE_LEN
    from .iterable_interface import IterableInterface

Native = IterableInterface
Row, Record = tuple, dict
FormattedRow = Tuple[str]
AnyField = Any
Column = Union[Name, Tuple[Name, Count], AnyField]
Columns = Optional[Iterable[Column]]


class SheetInterface(IterableInterface, ABC):
    @classmethod
    @abstractmethod
    def from_one_record(cls, record: Record, name: Name = EMPTY) -> Native:
        pass

    @classmethod
    @abstractmethod
    def from_records(cls, records: Iterable[Record], columns: Columns = None, name: Name = EMPTY) -> Native:
        pass

    @classmethod
    @abstractmethod
    def from_rows(cls, rows: Iterable[Row], columns: Columns, name: Name = EMPTY) -> Native:
        pass

    @abstractmethod
    def get_formatted_rows(self, with_title: bool = True, max_len: Count = DEFAULT_LINE_LEN) -> Iterator[FormattedRow]:
        pass

    @abstractmethod
    def get_lines(self, delimiter: str = REPR_DELIMITER) -> Iterable[str]:
        pass

    @abstractmethod
    def get_columns(self, including_lens: bool = False) -> list:
        pass

    @abstractmethod
    def get_column_names(self) -> list:
        pass

    @abstractmethod
    def get_column_lens(self) -> list:
        pass

    @abstractmethod
    def set_columns(self, columns: Iterable, inplace: bool = True) -> Native:
        pass
