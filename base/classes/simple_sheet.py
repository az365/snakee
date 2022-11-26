from abc import ABC
from typing import Optional, Iterable, Iterator, Sequence, Tuple, Union

try: # Assume we're a submodule in a package.
    from base.classes.typing import Name, Count, Class, Auto, AUTO, ARRAY_TYPES
    from base.constants.chars import EMPTY, REPR_DELIMITER, CROP_SUFFIX, SHORT_CROP_SUFFIX, DEFAULT_LINE_LEN
    from base.functions.arguments import get_name
    from base.abstract.simple_data import SimpleDataWrapper
    from base.interfaces.sheet_interface import SheetInterface, Record, Row, FormattedRow, Column, Columns
    from base.mixin.iter_data_mixin import IterDataMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Name, Count, Class, Auto, AUTO, ARRAY_TYPES
    from ..constants.chars import EMPTY, REPR_DELIMITER, CROP_SUFFIX, SHORT_CROP_SUFFIX, DEFAULT_LINE_LEN
    from ..functions.arguments import get_name
    from ..abstract.simple_data import SimpleDataWrapper
    from ..interfaces.sheet_interface import SheetInterface, Record, Row, FormattedRow, Column, Columns
    from ..mixin.iter_data_mixin import IterDataMixin

Native = Union[SimpleDataWrapper, IterDataMixin, SheetInterface]
SheetItems = Union[Iterable[Row], Iterable[Record]]


class SheetMixin(ABC):
    @classmethod
    def from_one_record(cls, record: Record, name: Name = EMPTY) -> Native:
        column_names = 'key', 'value'
        property_rows = list()
        for field in sorted(record):
            value = record[field]
            current_row = field, value
            property_rows.append(current_row)
        return cls.from_rows(rows=property_rows, columns=column_names, name=name)

    @classmethod
    def from_records(cls, records: Iterable[Record], columns: Columns = None, name: Name = EMPTY) -> Native:
        if Auto.is_defined(columns):
            column_names = cls._get_column_names_from_columns(columns)
        else:
            records = list(records)
            column_names = cls._get_column_names_from_records(records)
            columns = column_names
        rows = list()
        for record in records:
            row = [record.get(c) for c in column_names]
            rows.append(tuple(row))
        sheet_class = cls.get_class()
        return sheet_class(data=rows, columns=columns, name=name)

    @classmethod
    def from_rows(cls, rows: Iterable[Row], columns: Columns, name: Name = EMPTY) -> Native:
        if not Auto.is_defined(columns):
            rows = list(rows)
            if rows:
                count = len(rows[0])
                columns = list(range(count))
            else:
                columns = list()
        sheet_class = cls.get_class()
        return sheet_class(data=rows, columns=columns, name=name)

    def get_records(self) -> Iterator[Record]:
        columns = self.get_columns()
        for row in self.get_rows(with_title=False):
            yield Record(zip(columns, row))

    def get_rows(self, with_title: bool, upper_title: bool = True) -> Iterator[Row]:
        if with_title:
            yield self.get_title_row(upper_title=upper_title)
        yield from self.get_items()

    def get_title_row(self, upper_title: bool = False) -> Row:
        title_row = self.get_columns()
        if upper_title:
            title_row = [str(i).upper() for i in title_row]
        return Row(title_row)

    def get_formatted_rows(self, with_title: bool = True, max_len: Count = DEFAULT_LINE_LEN) -> Iterator[FormattedRow]:
        column_lens = self.get_column_lens(default=max_len)
        for row in self.get_rows(with_title=with_title, upper_title=True):
            formatted_row = list()
            assert self.get_columns() and column_lens, self.get_column_names_and_lens()
            for cell, max_cell_len in zip(row, column_lens):
                if max_cell_len is None:
                    max_cell_len = max_len
                cell_str = self._crop_cell(cell, max_cell_len)
                formatted_row.append(cell_str)
            yield tuple(formatted_row)

    def get_lines(self, delimiter: str = REPR_DELIMITER) -> Iterable[str]:
        placeholders = ['{:' + str(min_len) + '}' for min_len in self.get_column_lens()]
        formatter = delimiter.join(placeholders)
        for row in self.get_formatted_rows(with_title=True):
            yield formatter.format(row)

    def get_columns(self, including_lens: bool = False) -> list:
        if including_lens:
            return self.get_column_names_and_lens()
        else:
            return self.get_column_names()

    def get_column_names(self) -> list:
        return self._column_names

    def get_column_lens(self, default: Optional[int] = None) -> list:
        self.set_default_column_lens(default)
        return self._column_lens

    def get_column_names_and_lens(self) -> list[Tuple[Name, Count]]:
        names_and_lens = zip(self.get_columns(), self.get_column_lens())
        return list(names_and_lens)

    def set_default_column_lens(self, default: int) -> Native:
        count = len(self.get_columns())
        for i in range(count):
            if len(self._column_lens) < i + 1:
                self._column_lens.append(default)
            elif self._column_lens[i] is None:
                self._column_lens[i] = default
        return self

    def set_columns(self, columns: Iterable, inplace: bool = True) -> Native:
        if inplace:
            self._set_columns_inplace(columns)
            return self
        else:
            return SimpleSheet(data=self.get_data(), columns=columns, name=self.get_name(), caption=self.get_caption())

    def _set_items_inplace(self, items: SheetItems) -> None:
        items = list(items)
        if items:
            first_item = items[0]
            if isinstance(first_item, Row):
                columns = None
                rows = items
            elif isinstance(first_item, Record):
                columns = self.get_column_names() or self._get_column_names_from_records(items)
                rows = [tuple([i.get(c) for c in columns]) for i in items]
            else:
                raise TypeError(f'Expected items as Rows or Records, got {first_item} as {type(first_item)}')
            if columns:
                self._set_columns_inplace(columns)
        else:
            rows = items
        self.set_data(rows, inplace=True)

    @staticmethod
    def _get_name_and_len_from_column(column) -> Tuple[Name, Count]:
        is_name_len_pair = isinstance(column, ARRAY_TYPES) and not isinstance(column, str)
        if is_name_len_pair:
            if len(column) == 1:
                name, length = column[0], None
            elif len(column) == 2:
                name, length = column
            else:
                raise ValueError(f'Expected AnyField, Name or Tuple[Name, Count], got {column}')
        else:
            name = get_name(column)
            if hasattr(column, 'get_representation'):  # isinstance(column, AnyField):
                length = column.get_representation().get_max_value_len()
            else:
                length = None
        return name, length

    @classmethod
    def _get_column_names_from_columns(cls, columns: Iterable) -> list[Name]:
        column_names = list()
        for c in columns:
            name, _ = cls._get_name_and_len_from_column(c)
            column_names.append(name)
        return column_names

    @classmethod
    def _get_column_names_from_records(cls, records: Sequence[Record]) -> list[Name]:
        column_names = list()
        for r in records:
            for c in r:
                if c not in column_names:
                    column_names.append(c)
        return column_names

    @classmethod
    def _get_column_names_from_rows(cls, rows: Sequence[Row]) -> list[Name]:
        count = len(rows[0])
        column_names = list(range(count))
        return column_names

    @classmethod
    def _get_column_names_from_items(cls, items: Union[Sequence[Row], Sequence[Record]]) -> list[Name]:
        if items:
            first_item = items[0]
            if isinstance(first_item, Row):
                return cls._get_column_names_from_rows(items)
            elif isinstance(first_item, Record):
                return cls._get_column_names_from_records(items)
            else:
                raise TypeError(f'Expected items as Rows or Records, got {first_item} as {type(first_item)}')
        return []

    def _add_one_column_inplace(self, column) -> Native:
        name, length = self._get_name_and_len_from_column(column)
        self._column_names.append(name)
        self._column_lens.append(length)
        return self

    def _set_columns_inplace(self, columns: Optional[Iterable]) -> None:
        self._reset_columns()
        if columns:
            for c in columns:
                self._add_one_column_inplace(c)

    def _reset_columns(self) -> None:
        self._column_names = list()
        self._column_lens = list()

    columns = property(get_column_names, _set_columns_inplace, _reset_columns)

    @staticmethod
    def _crop_cell(value, max_len: Optional[int], crop_suffix: str = CROP_SUFFIX) -> str:
        cell_str = str(value)
        crop_len = len(crop_suffix)
        if max_len is not None:
            cell_len = len(cell_str)
            if cell_len > max_len:
                value_len = max_len - crop_len
                if value_len > 0:
                    cell_str = cell_str[:value_len] + crop_suffix
                elif max_len > 1:
                    cell_str = cell_str[:max_len - 1] + SHORT_CROP_SUFFIX
                else:
                    cell_str = cell_str[:max_len]
        return cell_str


class SimpleSheet(SimpleDataWrapper, IterDataMixin, SheetMixin, SheetInterface):
    _sheet_class: Class = None

    def __init__(self, data: Iterable, columns: Columns, name: str = EMPTY, caption: str = EMPTY):
        self._column_names = list()
        self._column_lens = list()
        super().__init__(list(), name=name, caption=caption)
        self._set_columns_inplace(columns)
        self._set_items_inplace(data)

    @classmethod
    def get_class(cls) -> Class:
        if not cls._sheet_class:
            cls._sheet_class = cls
        return cls._sheet_class

    @classmethod
    def set_class(cls, sheet_class: Class) -> Class:
        cls._sheet_class = sheet_class
        return sheet_class
