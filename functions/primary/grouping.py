from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from content.items.simple_items import (
        Record, MutableRecord, Row, MutableRow, ImmutableRow,
        Array, FieldName, Value,
    )
    from content.items.item_type import ItemType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...content.items.simple_items import (
        Record, MutableRecord, Row, MutableRow, ImmutableRow,
        Array, FieldName, Value,
    )
    from ...content.items.item_type import ItemType


def transpose_records_list(records_list: Iterable) -> MutableRecord:
    record_out = MutableRecord()
    for r in records_list:
        for k, v in r.items():
            record_out[k] = record_out.get(k, []) + [v]
    return record_out


def get_histograms(
        records: Array,
        fields: Optional[Iterable] = None,
        max_values: int = 25,
        ignore_none: bool = False,
) -> Generator:
    histograms = MutableRecord()
    for r in records:
        cur_fields = fields or r.keys()
        for f in cur_fields:
            if f not in histograms:
                histograms[f] = MutableRecord()
            cur_hist = histograms[f]
            cur_value = r.get(f)
            cur_count = cur_hist.get(cur_value, 0)
            can_add_new_key = len(cur_hist) < max_values
            if (cur_count or can_add_new_key) and (cur_value is not None or not ignore_none):
                cur_hist[cur_value] = cur_count + 1
    for k, v in histograms.items():
        yield k, v


def sum_by_keys(records: Iterable, keys: Array, counters: Array) -> Generator:
    result = MutableRecord()
    for r in records:
        cur_key = ImmutableRow([r.get(k) for k in keys])
        if cur_key not in result:
            result[cur_key] = MutableRecord()
        for c in counters:
            result[cur_key][c] = result[cur_key].get(c, 0) + r.get(c, 0)
    yield from result.items()


def get_first_values(records: Iterable, fields: Array) -> MutableRecord:
    first_values = MutableRecord()
    empty_fields = fields.copy()
    for r in records:
        added_fields = list()
        for f in empty_fields:
            v = r.get(f)
            if v:
                first_values[f] = v
                added_fields.append(f)
        for f in added_fields:
            empty_fields.remove(f)
    return first_values


def get_group_name(value: Value, default: FieldName = 'other', **kwargs) -> FieldName:
    for group_name, group_values in kwargs.items():
        if value in group_values:
            return group_name
    return default


def fold_rows(
        list_rows: Array,
        key_columns: Array,
        list_columns: Iterable,
        skip_missing: bool = False,
) -> Optional[ImmutableRow]:
    if list_rows:
        row_out = MutableRow()
        first_row = list_rows[0]
        for c in key_columns:
            if isinstance(c, Callable):
                row_out.append(c(first_row))
            elif isinstance(c, int):
                try:
                    row_out.append(first_row[c])
                except IndexError:
                    if skip_missing:
                        row_out.append(None)
                    else:
                        raise IndexError('fold_rows(): first row has no column {}: {}'.format(c, first_row))
            else:
                raise TypeError('fold_rows(): expected function or column number, got {}'.format(c))
        for c_in in list_columns:
            row_out.append(list())
            c_out = len(row_out) - 1
            for r_in in list_rows:
                if isinstance(c_in, Callable):
                    row_out[c_out].append(c_in(r_in))
                elif isinstance(c_in, int):
                    try:
                        row_out[c_out].append(r_in[c_in])
                    except IndexError:
                        if skip_missing:
                            row_out[c_out].append(None)
                        else:
                            raise IndexError('fold_rows(): row has no column {}: {}'.format(c_in, r_in))
                else:
                    raise TypeError('fold_rows(): expected function or column number, got {}'.format(c_in))
        return ImmutableRow(row_out)


def fold_records(
        list_records: Array,
        key_fields: Iterable,
        list_fields: Iterable,
        skip_missing: bool = False,
) -> MutableRecord:
    rec_out = MutableRecord()
    for f in key_fields:
        if hasattr(f, 'get_name'):
            f = f.get_name()
        if list_records:
            first_item = list_records[0]
            rec_out[f] = first_item.get(f)
        elif not skip_missing:
            raise ValueError('fold_lists(): list_records is empty')
    for f in list_fields:
        if hasattr(f, 'get_name'):
            f = f.get_name()
        rec_out[f] = [r.get(f) for r in list_records]
    return rec_out


def fold_lists(
        list_items: Array,
        key_fields: Array,
        list_fields: Array,
        as_pairs: bool = False,
        skip_missing: bool = False,
        item_type: Optional[ItemType] = None,
) -> Union[Row, Record]:
    if list_items:
        if not Auto.is_defined(item_type):
            first_item = list_items[0]
            item_type = ItemType.detect(first_item)
        if item_type == ItemType.Record:
            return fold_records(list_items, key_fields, list_fields, skip_missing=skip_missing)
        elif item_type in (ItemType.Row, ItemType.StructRow):
            if as_pairs:
                list_items = list_items[1]  # list is in value from KeyValue-pair
            return fold_rows(list_items, key_fields, list_fields, skip_missing=skip_missing)
        else:
            raise ValueError('fold_lists(): expected Record, Row or StructRow, got {}'.format(item_type))
    elif not skip_missing:
        raise ValueError('fold_lists(): list_items must be non-empty')


def unfold_lists(record: Record, fields: Array, number_field: FieldName = 'n', default_value: Value = 0) -> Generator:
    rec_common = {k: v for k, v in record.items() if k not in fields}
    fold_values = [record.get(f, []) for f in fields]
    if default_value is not None:
        max_len = max([len(a or []) for a in fold_values])
        fold_values = [list(a or []) + [default_value] * (max_len - len(a or [])) for a in fold_values]
    for n, unfold_values in enumerate(zip(*fold_values)):
        rec_out = rec_common.copy()
        rec_out.update({k: v for k, v in zip(fields, unfold_values)})
        if number_field:
            rec_out[number_field] = n
        yield rec_out
