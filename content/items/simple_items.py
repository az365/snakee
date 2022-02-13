from abc import ABC, abstractmethod
from collections import Mapping, OrderedDict
from typing import Callable, Iterable, Sequence, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.classes.typing import FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES
    from base.functions.arguments import get_name
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import Auto, AUTO
    from ...base.classes.typing import FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES
    from ...base.functions.arguments import get_name


class SimpleRowInterface(Sequence, ABC):
    @abstractmethod
    def __add__(self, other) -> Iterable:
        pass


class FrozenDict(Mapping):
    def __init__(self, *args, **kwargs):
        self._d = dict(*args, **kwargs)
        self._hash = None

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]

    def __hash__(self):
        if self._hash is None:
            hash_ = 0
            for pair in self.items():
                hash_ ^= hash(pair)
            self._hash = hash_
        return self._hash


MutableRow = list
ImmutableRow = tuple
SimpleRow = Union[MutableRow, ImmutableRow]
Row = Union[SimpleRow, SimpleRowInterface]

MutableRecord = dict
ImmutableRecord = Mapping  # FrozenDict
Record = Union[MutableRecord, ImmutableRecord]

RecRow = OrderedDict

Line = str
SimpleSelectableItem = Union[Row, Record, OrderedDict]
SimpleItem = Union[SimpleSelectableItem, Line]
Item = Union[SimpleItem, Any]

ROW_SUBCLASSES = MutableRow, ImmutableRow, SimpleRowInterface
RECORD_SUBCLASSES = MutableRecord, ImmutableRecord, RecRow
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
        field = get_name(field)
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
