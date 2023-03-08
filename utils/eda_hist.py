from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        LoggerInterface, RegularStreamInterface, StreamType, ItemType, MutableRecord, LoggingLevel,
        Message, Count, Array,
    )
    from functions.primary import numeric as nm
    from functions.secondary import all_secondary_functions as fs
    from streams.stream_builder import StreamBuilder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import (
        LoggerInterface, RegularStreamInterface, StreamType, ItemType, MutableRecord, LoggingLevel,
        Message, Count, Array,
    )
    from ..functions.primary import numeric as nm
    from ..functions.secondary import all_secondary_functions as fs
    from ..streams.stream_builder import StreamBuilder

Data = Union[RegularStreamInterface, Iterable]

DEFAULT_STEP = 1000000
TOP_COUNT = 3


def get_hist_records(
        stream: RegularStreamInterface,
        fields: Iterable,
        logger: Optional[LoggerInterface] = None,
        msg: Optional[Message] = None,
) -> Iterable:
    if logger is not None:
        logger.log(msg=msg if msg else 'calc hist in memory...', level=LoggingLevel.Info)
    dict_hist = {f: dict() for f in fields}
    for i in stream.get_items():
        for f in fields:
            v = i.get(f)
            if isinstance(v, list):
                v = tuple(v)
            dict_hist[f][v] = dict_hist[f].get(v, 0) + 1
    for f in fields:
        cur_hist = dict_hist[f]
        for v, c in sorted(cur_hist.items(), key=fs.second(), reverse=True):
            yield MutableRecord(field=f, value=v, count=c)


def hist(
        data: Data,
        *fields,
        in_memory: Optional[bool] = None,
        step: Count = DEFAULT_STEP,
        logger: Optional[LoggerInterface] = None,
        msg: Optional[Message] = None,
) -> RegularStreamInterface:
    output_columns = 'field', 'value', 'count', 'share', 'total_count'
    stream = _build_stream(data)
    total_count = stream.get_count()
    if in_memory is None:
        in_memory = stream.is_in_memory()
    if logger is None:
        logger = stream.get_logger(delayed=True)
    if in_memory or len(fields) > 1:
        stream = stream.stream(
            get_hist_records(stream, fields, logger=logger, msg=msg),
            stream_type=ItemType.Record,
        )
    else:
        stream = stream if len(fields) <= 1 else stream.tee_stream()
        f = fields[0]
        if logger:
            logger.log(f'Calc hist for field {f}...')
        stream = stream.to_stream(
            stream_type=ItemType.Record,
            columns=fields,
        ).select(
            f,
        ).group_by(
            f,
            values=['-'],
            step=step,
        ).select(
            field=lambda r, k=f: k,
            value=f,
            count=('-', len),
        ).sort(
            'value',
            reverse=True,
        )
    if not total_count:
        stream = stream.to_memory()
        any_single_field = fields[0]
        total_count = sum(stream.filter(field=any_single_field).get_one_column_values('count'))
    stream = stream.select(
        '*',
        total_count=fs.const(total_count),
        share=('count', 'total_count', fs.div()),
    ).set_struct(
        output_columns,
    )
    return _assume_native(stream)


def stat(
        data: Data,
        *fields,
        in_memory: bool = True,
        take_hash: bool = True,
        count: Count = TOP_COUNT,
        msg: Optional[Message] = None,
) -> RegularStreamInterface:
    return hist(
        data,
        *fields,
        in_memory=in_memory,
        msg=msg,
    ).group_by(
        'field',
        values=['value', 'count', 'total_count', '-'],
        take_hash=take_hash,
    ).select(
        '*',
        items_count=('-', len),
        total_count=('total_count', fs.first()),
        zip=('value', 'count', lambda v, c: list(zip(v, c))),
    ).select(
        'field',
        'total_count',
        defined_count=('zip', lambda z: sum(c0 for k0, c0 in z if k0 is not None)),
        nonzero_count=('zip', lambda z: sum(c0 for k0, c0 in z if k0)),
        defined_share=('defined_count', 'total_count', fs.div()),
        nonzero_share=('nonzero_count', 'total_count', fs.div()),
        uniq_count=('value', len),
        items_per_value=('nonzero_count', 'uniq_count', fs.div()),
        min=('value', min),
        max=('value', max),
        avg=('value', fs.avg()),
        median=('value', fs.median()),
        firstN=('value', 'count', lambda k, c: [(k0, c0) for k0, c0 in list(zip(k, c))[:count]]),
        topN=(
            'value', 'count',
            lambda k, c: [(k0, c0) for k0, c0 in sorted(zip(k, c), key=lambda i: i[1], reverse=True)[:count]],
        ),
    )


def hist_by_cat(data: Data, cat_fields: Array, hist_fields: Array):
    return _build_stream(data).group_to_pairs(
        cat_fields,
        verbose=False,
    ).map_values(
        lambda d: hist(d, *hist_fields).get_items(),
    ).ungroup_values()


def stat_by_cat(data: Data, cat_fields, hist_fields):
    return _build_stream(data).group_to_pairs(
        cat_fields,
        verbose=False,
    ).map(
        lambda i: (
            dict(zip(cat_fields, (i[0] if isinstance(i[0], Iterable) else [i[0]]))),
            stat(i[1], *hist_fields, msg=f'Processing key {i[0]}...').get_items()
        ),
    ).ungroup_values(
    ).map_to_type(
        lambda i: _merge_two_records(*i),
        stream_type=ItemType.Record,
    )


def _merge_two_records(r1, r2):
    return {k: v for k, v in (list(r1.items()) + list(r2.items()))}


def _build_stream(data: Data) -> RegularStreamInterface:
    if hasattr(data, 'is_file'):
        if data.is_file():
            if hasattr(data, 'to_record_stream'):
                return data.to_record_stream()
    if isinstance(data, RegularStreamInterface):
        stream = data
    else:
        stream = StreamBuilder.stream(data, check=False)
    return stream


def _assume_native(stream) -> RegularStreamInterface:
    return stream
