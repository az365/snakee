try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated, deprecated_with_alternative
    from content.items.item_type import ItemType
    from functions.primary import text as tx, grouping as gr
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..utils.decorators import deprecated, deprecated_with_alternative
    from ..content.items.item_type import ItemType
    from ..functions.primary import text as tx, grouping as gr


@deprecated_with_alternative('functions.primary.text.split_csv_row()')
def split_csv_row(line, delimiter=None):
    return tx.split_csv_row(line, delimiter=delimiter)


@deprecated
def apply_dict_to_field(record, field, dict_to_apply, default=None):
    value = record.get(field)
    record[field] = dict_to_apply.get(value, default or value)
    return record


@deprecated
def add_fields(record, additional_fields):
    record.update(additional_fields)
    return record


@deprecated
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


@deprecated
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


@deprecated_with_alternative('functions.primary.grouping.transpose_records_list()')
def transpose_records_list(records_list):
    return gr.transpose_records_list(records_list)


@deprecated_with_alternative('functions.primary.grouping.get_histograms()')
def get_histograms(records, fields=tuple(), max_values=25, ignore_none=False):
    return gr.get_histograms(records, fields=fields, max_values=max_values, ignore_none=ignore_none)


@deprecated_with_alternative('functions.primary.text.remove_extra_spaces()')
def remove_extra_spaces(text):
    return tx.remove_extra_spaces(text)


@deprecated_with_alternative('functions.primary.text.norm_text()')
def norm_text(text):
    return tx.norm_text(text)


@deprecated_with_alternative('functions.primary.grouping.sum_by_keys()')
def sum_by_keys(records, keys, counters):
    return gr.sum_by_keys(records, keys=keys, counters=counters)


@deprecated_with_alternative('functions.primary.grouping.get_first_values()')
def get_first_values(records, fields):
    return gr.get_first_values(records, fields=fields)


@deprecated_with_alternative('functions.primary.grouping.fold_lists()')
def fold_lists(list_items, key_fields, list_fields, skip_missing=False, item_type=None):
    return gr.fold_lists(list_items, key_fields, list_fields, skip_missing=skip_missing, item_type=item_type)


@deprecated_with_alternative('functions.primary.grouping.unfold_lists()')
def unfold_lists(record, fields, number_field='n', default_value=0):
    return gr.unfold_lists(record, fields=fields, number_field=number_field, default_value=default_value)
