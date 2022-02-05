from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StreamType, RegularStream, LoggerInterface,
        AUTO, Auto, AutoBool, Count, Message, Array,
    )
    from functions.primary import numeric as nm
    from functions.secondary import all_secondary_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import (
        StreamType, RegularStream, LoggerInterface,
        AUTO, Auto, AutoBool, Count, Message, Array,
    )
    from ..functions.primary import numeric as nm
    from ..functions.secondary import all_secondary_functions as fs

Data = Union[RegularStream, Iterable]

DEFAULT_STEP = 1000000
TOP_COUNT = 3


def get_hist_records(
        stream: RegularStream,
        fields: Iterable,
        in_memory: AutoBool = AUTO,
        logger: Union[LoggerInterface, Auto] = AUTO,
        msg: Optional[Message] = None,
) -> Iterable:
    if Auto.is_defined(logger):
        logger.log(msg if msg else 'calc hist in memory...')
    dict_hist = {f: dict() for f in fields}
    for i in stream.get_items():
        for f in fields:
            v = i.get(f)
            if isinstance(v, list):
                v = tuple(v)
            dict_hist[f][v] = dict_hist[f].get(v, 0) + 1
    for f in fields:
        cur_hist = dict_hist[f]
        for v, c in cur_hist.items():
            yield dict(field=f, value=v, count=c)


def hist(
        data: Data,
        *fields,
        in_memory: AutoBool = AUTO,
        step: Count = DEFAULT_STEP,
        logger: Union[LoggerInterface, Auto] = AUTO,
        msg: Optional[Message] = None,
) -> RegularStream:
    stream = _stream(data)
    total_count = stream.get_count()
    in_memory = Auto.acquire(in_memory, stream.is_in_memory())
    logger = Auto.acquire(logger, stream.get_logger, delayed=True)
    # if in_memory:
    if in_memory or len(fields) > 1:
        stream = stream.stream(
            get_hist_records(stream, fields, in_memory=in_memory, logger=logger, msg=msg),
            stream_type='RecordStream',
        )
    else:
        stream = stream if len(fields) <= 1 else stream.tee_stream()
        f = fields[0]
        if logger:
            logger.log('Calc hist for field {}...'.format(f))
        stream = stream.to_stream(
            stream_type='RecordStream',
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
        ).sort('value')
    if not total_count:
        stream = stream.to_memory()
        total_count = sum(stream.filter(field=fields[0]).get_one_column_values('count'))
    stream = stream.select(
        '*',
        total_count=fs.const(total_count),
        share=('count', 'total_count', lambda c, t: c / t if t else None),
    )
    return _assume_native(stream)


def stat(
        data: Data,
        *fields,
        in_memory: bool = True,
        take_hash: bool = True,
        count: Count = TOP_COUNT,
        msg: Optional[Message] = None,
) -> RegularStream:
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
        defined_share=('defined_count', 'total_count', lambda d, c: d / c if c else None),
        nonzero_share=('nonzero_count', 'total_count', lambda d, c: d / c if c else None),
        uniq_count=('value', len),
        items_per_value=('nonzero_count', 'uniq_count', lambda n, u: n / u if u else None),
        min=('value', min),
        max=('value', max),
        avg=('value', lambda v: nm.mean([n for n in v if isinstance(n, (int, float))])),
        median=('value', lambda v: nm.median([n for n in v if isinstance(n, (int, float))])),
        firstN=('value', 'count', lambda k, c: [(k0, c0) for k0, c0 in list(zip(k, c))[:count]]),
        topN=(
            'value', 'count',
            lambda k, c: [(k0, c0) for k0, c0 in sorted(zip(k, c), key=lambda i: i[1], reverse=True)[:count]],
        ),
    )


def hist_by_cat(data: Data, cat_fields: Array, hist_fields: Array):
    return _stream(data).group_to_pairs(
        cat_fields,
        verbose=False,
    ).map_values(
        lambda d: hist(d, *hist_fields).get_items(),
    ).ungroup_values()


def stat_by_cat(data: Data, cat_fields, hist_fields):
    return _stream(data).group_to_pairs(
        cat_fields,
        verbose=False,
    ).map(
        lambda i: (
            dict(zip(cat_fields, (i[0] if isinstance(i[0], Iterable) else [i[0]]))),  ###
            stat(
                i[1],
                *hist_fields,
                msg='Processing key {}...'.format(i[0]),
            ).get_items()
        ),
    ).ungroup_values(
    ).map_to(
        lambda i: _merge_two_records(*i),
        stream_type=StreamType.RecordStream,
    )


def _merge_two_records(r1, r2):
    return {k: v for k, v in (list(r1.items()) + list(r2.items()))}


def _stream(data: Data) -> RegularStream:
    if hasattr(data, 'is_file'):
        if data.is_file():
            if hasattr(data, 'to_record_stream'):
                return data.to_record_stream()
    if isinstance(data, RegularStream):
        stream = data
    else:
        stream_class = StreamType.RecordStream.get_class()
        stream = stream_class(data, check=False)
    return stream


def _assume_native(stream) -> RegularStream:
    return stream
