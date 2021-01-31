try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as fx
    from utils import algo
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..streams import stream_classes as fx
    from . import algo
    from ..functions import all_functions as fs


IGNORE_CYCLIC_DEPENDENCIES = False


def process_description(d):
    if callable(d):
        function, inputs = d, list()
    elif isinstance(d, (list, tuple)):
        if callable(d[0]):
            function, inputs = d[0], d[1:]
        elif callable(d[-1]):
            inputs, function = d[:-1], d[-1]
        else:
            inputs, function = d, lambda *a: tuple(a)
    else:
        inputs, function = [d], lambda v: v
    return function, inputs


def topologically_sorted(expressions, ignore_cycles=IGNORE_CYCLIC_DEPENDENCIES, logger=None):
    unordered_fields = list()
    unresolved_dependencies = dict()
    for field, description in expressions.items():
        unordered_fields.append(field)
        _, dependencies = process_description(description)
        unresolved_dependencies[field] = [
            d for d in dependencies
            if d in expressions.keys() and d != field
        ]
    ordered_fields = algo.topologically_sorted(
        nodes=unordered_fields,
        edges=unresolved_dependencies,
        ignore_cycles=ignore_cycles,
        logger=logger,
    )
    return [(f, expressions[f]) for f in ordered_fields]


def flatten_descriptions(*fields, **expressions):
    descriptions = list(fields)
    logger = expressions.pop('logger', None)
    ignore_cycles = logger is not None
    for k, v in topologically_sorted(expressions, ignore_cycles=ignore_cycles, logger=logger):
        if isinstance(v, list):
            descriptions.append([k] + v)
        elif isinstance(v, tuple):
            descriptions.append([k] + list(v))
        else:
            descriptions.append([k] + [v])
    return descriptions


def value_from_row(row, description):
    if callable(description):
        return description(row)
    elif isinstance(description, (list, tuple)):
        function, columns = process_description(description)
        values = [row[f] for f in columns]
        return function(*values)
    elif isinstance(description, int):
        return row[description]
    else:
        message = 'field description must be int, callable or tuple ({} as {} given)'
        raise TypeError(message.format(description, type(description)))


def value_from_record(record, description, logger=None, skip_errors=True):
    if callable(description):
        return description(record)
    elif isinstance(description, (list, tuple)):
        function, fields = process_description(description)
        values = [record.get(f) for f in fields]
        try:
            return function(*values)
        except BaseException as e:
            if logger:
                level = 30 if skip_errors else 40
                message = 'Error while processing function {} over fields {} with values {}.'
                logger.log(msg=message.format(function.__name__, fields, values), level=level)
            if not skip_errors:
                raise e
    else:
        return record.get(description)


def value_from_any(item, description):
    if callable(description):
        return description(item)
    elif isinstance(description, (list, tuple)):
        function, fields = process_description(description)
        values = fs.values_by_keys(fields)(item)
        return function(*values)
    else:
        return fs.value_by_key(description)(item)


def tuple_from_record(record, descriptions):
    return tuple([value_from_record(record, d) for d in descriptions])


def row_from_row(row_in, *descriptions):
    row_out = [None] * len(descriptions)
    c = 0
    for d in descriptions:
        if d == '*':
            row_out = row_out[:c] + list(row_in) + row_out[c + 1:]
            c += len(row_in)
        else:
            row_out[c] = value_from_row(row_in, d)
            c += 1
    return tuple(row_out)


def row_from_any(item_in, *descriptions):
    row_out = [None] * len(descriptions)
    c = 0
    for desc in descriptions:
        if desc == '*':
            if fx.is_row(item_in):
                row_out = row_out[:c] + list(item_in) + row_out[c + 1:]
                c += len(item_in)
            else:
                row_out[c] = item_in
                c += 1
        else:
            row_out[c] = value_from_any(item_in, desc)
            c += 1
    return tuple(row_out)


def record_from_any(item_in, *descriptions):
    rec_out = dict()
    for desc in descriptions:
        assert isinstance(desc, (list, tuple)) and len(desc) > 1, 'for AnyStream items description {} is not applicable'
        f_out = desc[0]
        if len(desc) == 2:
            f_in = desc[1]
            if callable(f_in):
                rec_out[f_out] = f_in(item_in)
            else:
                rec_out[f_out] = rec_out.get(f_in)
        else:
            fs_in = desc[1:]
            rec_out[f_out] = value_from_record(rec_out, fs_in)
    return rec_out


def record_from_record(rec_in, *descriptions):
    record = rec_in.copy()
    fields_out = list()
    for desc in descriptions:
        if desc == '*':
            fields_out += list(rec_in.keys())
        elif isinstance(desc, (list, tuple)):
            if len(desc) > 1:
                f_out = desc[0]
                fs_in = desc[1] if len(desc) == 2 else desc[1:]
                record[f_out] = value_from_record(record, fs_in)
                fields_out.append(f_out)
            else:
                raise ValueError('incorrect field description: {}'.format(desc))
        else:  # desc is field name
            if desc not in record:
                record[desc] = None
            fields_out.append(desc)
    return {f: record[f] for f in fields_out}
