from abc import ABC, abstractmethod
from typing import Iterable, Callable, Union, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Value = Any
Array = Union[list, tuple]

ARRAY_TYPES = list, tuple
ROW_SUBCLASSES = ARRAY_TYPES
RECORD_SUBCLASSES = dict,
STAR = '*'


class SimpleRowInterface(ABC):
    @abstractmethod
    def __iter__(self) -> Iterable:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __getitem__(self, item) -> Value:
        pass

    @abstractmethod
    def __add__(self, other) -> Iterable:
        pass


SimpleRow = Array
Row = Union[SimpleRow, SimpleRowInterface]
Record = dict
Line = str
SimpleSelectableItem = Union[Row, Record]
SimpleItem = Union[SimpleSelectableItem, Line]


def get_field_value_from_row(
        column: int, row: Row,
        default: Value = None, skip_missing: bool = True,
) -> Value:
    if column < len(row) or not skip_missing:
        return row[column]
    else:
        return default


def get_field_value_from_record(
        field: Union[FieldID, Callable], record: Record,
        default: Value = None, skip_missing: bool = True,
) -> Value:
    if isinstance(field, Callable):
        return field(record)
    else:
        field = arg.get_name(field)
    if skip_missing:
        return record.get(field, default)
    else:
        return record[field]
