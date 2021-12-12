from typing import Callable, Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from items.item_type import ItemType
    from functions.primary import text as tx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..utils.decorators import deprecated_with_alternative
    from ..items.item_type import ItemType
    from ..functions.primary import text as tx


@deprecated_with_alternative('functions.primary.text.split_csv_row()')
def split_csv_row(line, delimiter=None):
    return tx.split_csv_row(line, delimiter=delimiter)


def apply_dict_to_field(record, field, dict_to_apply, default=None):
    value = record.get(field)
    record[field] = dict_to_apply.get(value, default or value)
    return record


def add_fields(record, additional_fields):
    record.update(additional_fields)
    return record


def crop_cells(row, max_len=33, substitute='...', crop_str_only=True):
    result = list()
    subst_len = len(substitute)
    limit_len = max_len - subst_len
    assert limit_len > 0
    for cell in row:
        output_cell = cell
        if isinstance(cell, str) or not crop_str_only:
            str_cell = str(cell)
            if len(str_cell) > max_len:
                output_cell = str_cell[:limit_len] + substitute
        result.append(output_cell)
    return result


def union_duplicate_fields(record, list_duplicate_fields=(('a1', 'a2'), ('b1', 'b2', 'b3'))):
    for duplicate_fields_group in list_duplicate_fields:
        main_field = duplicate_fields_group[0]
        first_value = None, None
        for field in duplicate_fields_group:
            cur_value = record.get(field)
            if cur_value is not None:
                first_value = cur_value
        if first_value:
            record[main_field] = first_value
            for field in duplicate_fields_group[1:]:
                record.pop(field, None)
    return record


def transpose_records_list(records_list):
    record_out = dict()
    for r in records_list:
        for k, v in r.items():
            record_out[k] = record_out.get(k, []) + [v]
    return record_out


def get_histograms(records, fields=tuple(), max_values=25, ignore_none=False):
    histograms = dict()
    for r in records:
        for f in fields or r.keys():
            if f not in histograms:
                histograms[f] = dict()
            cur_hist = histograms[f]
            cur_value = r.get(f)
            cur_count = cur_hist.get(cur_value, 0)
            can_add_new_key = len(cur_hist) < max_values
            if (cur_count or can_add_new_key) and (cur_value is not None or not ignore_none):
                cur_hist[cur_value] = cur_count + 1
    for k, v in histograms.items():
        yield k, v


@deprecated_with_alternative('functions.primary.text.remove_extra_spaces()')
def remove_extra_spaces(text):
    return tx.remove_extra_spaces(text)


@deprecated_with_alternative('functions.primary.text.norm_text()')
def norm_text(text):
    return tx.norm_text(text)


def sum_by_keys(records, keys, counters):
    result = dict()
    for r in records:
        cur_key = tuple([r.get(k) for k in keys])
        if cur_key not in result:
            result[cur_key] = dict()
        for c in counters:
            result[cur_key][c] = result[cur_key].get(c, 0) + r.get(c, 0)
    yield from result.items()


def get_first_values(records, fields):
    dict_first_values = dict()
    empty_fields = fields.copy()
    for r in records:
        added_fields = list()
        for f in empty_fields:
            v = r.get(f)
            if v:
                dict_first_values[f] = v
                added_fields.append(f)
        for f in added_fields:
            empty_fields.remove(f)
    return dict_first_values


def fold_rows(list_rows, key_columns, list_columns, skip_missing=False) -> tuple:
    if list_rows:
        row_out = list()
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
                    row_out[c_out].append(c_in(c_in))
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
        return tuple(row_out)


def fold_records(list_records, key_fields, list_fields, skip_missing=False) -> dict:
    rec_out = dict()
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


def fold_lists(list_items: Union[list, tuple], key_fields, list_fields, skip_missing=False, item_type=None):
    if list_items:
        if not arg.is_defined(item_type):
            first_item = list_items[0]
            item_type = ItemType.detect(first_item)
        if item_type == ItemType.Record:
            return fold_records(list_items, key_fields, list_fields, skip_missing=skip_missing)
        elif item_type in (ItemType.Row, ItemType.StructRow):
            return fold_rows(list_items, key_fields, list_fields, skip_missing=skip_missing)
        else:
            raise ValueError('fold_lists(): expected Record, Row or StructRow, got {}'.format(item_type))
    elif not skip_missing:
        raise ValueError('fold_lists(): list_items must be non-empty')


def unfold_lists(record, fields, number_field='n', default_value=0):
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
