from abc import ABC, abstractmethod
from typing import Type, Callable, Iterable, Union, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Value = Any
Class = Union[Type, Callable]
Array = Union[list, tuple]
ARRAY_TYPES = list, tuple


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
Item = Union[SimpleItem, Any]

ROW_SUBCLASSES = list, tuple, SimpleRowInterface
RECORD_SUBCLASSES = dict,
LINE_SUBCLASSES = str,
STAR = '*'


def is_line(item: Item) -> bool:
    return isinstance(item, LINE_SUBCLASSES)


def is_row(item: Item) -> bool:
    return isinstance(item, ROW_SUBCLASSES)


def is_record(item: Item) -> bool:
    return isinstance(item, RECORD_SUBCLASSES)


def get_field_value_from_struct_row(
        field: Union[FieldID, Callable], row: Union[Row, Any],
        default: Value = None, skip_missing: bool = True,
        struct=None,
) -> Value:
    if isinstance(field, Callable):
        func = field
        return func(row)
    elif hasattr(field, 'get_function'):
        func = field.get_function()
        return func(row)
    elif hasattr(row, 'get_value'):
        return row.get_value(field, default=default, skip_missing=True)
    elif isinstance(field, int):
        column = field
    elif struct and isinstance(field, str):
        column = struct.get_field_position(field)
    else:
        raise TypeError('Expected Field, Column or Callable, got {}'.format(field))
    return get_field_value_from_row(column, row, default=default, skip_missing=skip_missing)


def get_field_value_from_row(
        column: Union[FieldNo, Callable], row: Row,
        default: Value = None, skip_missing: bool = True,
) -> Value:
    if isinstance(column, Callable):
        return column(row)
    elif column < len(row) or not skip_missing:
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


def merge_two_rows(first: Row, second: Item) -> Row:
    if second is None:
        result = first
    elif is_row(second):
        result = tuple(list(first) + list(second))
    else:
        result = tuple(list(first) + [second])
    return result


def merge_two_records(first: Record, second: Item, default_right_name: str = '_right') -> Record:
    result = first.copy()
    if is_record(second):
        result.update(second)
    else:
        result[default_right_name] = second
    return result
