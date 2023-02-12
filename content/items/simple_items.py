from abc import ABC, abstractmethod
from typing import Callable, Iterable, Sequence, Union, Any
from collections import OrderedDict
try:
    from collections import Mapping
except ImportError:
    from collections.abc import Mapping

try:  # Assume we're a submodule in a package.
    from base.classes.typing import FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES
    from base.constants.chars import ALL
    from base.functions.arguments import get_name
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES
    from ...base.constants.chars import ALL
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
SimpleRecord = Union[MutableRecord, ImmutableRecord]
Record = SimpleRecord

RecRow = OrderedDict

Line = str
SimpleSelectableItem = Union[SimpleRow, SimpleRecord, OrderedDict]
SimpleItem = Union[SimpleSelectableItem, Line]
SelectableItem = Union[Row, Record, RecRow]
Item = Union[SelectableItem, Line, Any]

ROW_SUBCLASSES = MutableRow, ImmutableRow, SimpleRowInterface
RECORD_SUBCLASSES = MutableRecord, ImmutableRecord, RecRow
LINE_SUBCLASSES = str,
FULL_ITEM_FIELD = 'item'


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
    if isinstance(row, Sequence):
        if column < len(row) or not skip_missing:
            return row[column]
    elif column == 0:
        return row
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
        if record is None:
            return None
        try:
            return record.get(field, default)
        except AttributeError as e:
            raise AttributeError(f'{e}: {record}')
    else:
        return record[field]


def merge_two_rows(first: Row, second: Item, ordered: bool = False, frozen: bool = True) -> SimpleRow:
    if isinstance(first, Iterable):
        first = list(first)
    else:
        first = [first]
    if is_row(second):
        result = first
        if ordered:
            for n, v in enumerate(second):
                if v is not None:
                    while len(result) <= n:
                        result.append(None)
                    result[n] = v
        else:
            result += list(second)
    else:
        result = first + [second]
    if frozen and isinstance(result, Iterable):
        result = tuple(result)
    return result


def merge_two_records(first: Record, second: Item, default_right_name: str = '_right') -> Record:
    result = first.copy()
    if is_record(second):
        result.update(second)
    else:
        result[default_right_name] = second
    return result
