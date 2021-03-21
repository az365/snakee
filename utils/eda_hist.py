from typing import Iterable, Union

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sc
    from utils import (
        arguments as arg,
        numeric as nm,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..streams import stream_classes as sc
    from . import (
        arguments as arg,
        numeric as nm,
    )


Stream = Union[sc.LocalStream, sc.ColumnarMixin]


def get_hist_records(stream: Stream, fields: Iterable, in_memory=arg.DEFAULT, logger=arg.DEFAULT) -> Iterable:
    logger = arg.undefault(logger, stream.get_logger())
    print('\ncalc hist in memory...')  # tmp
    dict_hist = {f: dict() for f in fields}
    for i in stream.get_items():
        for f in fields:
            v = i.get(f)
            dict_hist[f][v] = dict_hist[f].get(v, 0) + 1
    for f in fields:
        cur_hist = dict_hist[f]
        for v, c in cur_hist.items():
            yield dict(field=f, value=v, count=c)


def hist(stream: Stream, *fields, in_memory=arg.DEFAULT, logger=arg.DEFAULT) -> Stream:
    in_memory = arg.undefault(in_memory, stream.is_in_memory())
    logger = arg.undefault(logger, stream.get_logger, delayed=True)
    # if in_memory:
    if in_memory or len(fields) > 1:
        return stream.stream(
            get_hist_records(stream, fields, in_memory=in_memory, logger=logger),
            stream_type='RecordStream',
        ).to_memory()
    else:
        stream = stream if len(fields) <= 1 else stream.tee_stream()
        f = fields[0]
        if logger:
            logger.log('Calc hist for field {}...'.format(f))
        return stream.to_stream(
            stream_type='RecordStream',
        ).group_by(
            f,
            values=['-']
        ).select(
            field=lambda r, k=f: k,
            value=f,
            count=('-', len),
        ).sort('value')


def stat(stream: Stream, *fields, in_memory=True, take_hash=True, count=3) -> Stream:
    return hist(
        stream,
        *fields,
        in_memory=in_memory,
    ).group_by(
        'field',
        values=['value', 'count', '-'],
        take_hash=take_hash,
    ).select(
        '*',
        items_count=('-', len),
        zip=('value', 'count', lambda v, c: list(zip(v, c))),
    ).select(
        'field',
        defined_count=('zip', lambda z: sum(c0 for k0, c0 in z if k0 is not None)),
        nonzero_count=('zip', lambda z: sum(c0 for k0, c0 in z if k0)),
        defined_share=('defined_count', 'items_count', lambda d, c: d / c if c else None),
        nonzero_share=('nonzero_count', 'items_count', lambda d, c: d / c if c else None),
        uniq_count=('value', len),
        items_per_value=('nonzero_count', 'uniq_count', lambda n, u: n / u if u else None),
        min=('value', min),
        max=('value', max),
        avg=('value', lambda v: nm.mean([n for n in v if isinstance(v, (int, float))])),
        median=('value', lambda v: nm.median([n for n in v if isinstance(v, (int, float))])),
        firstN=('value', 'count', lambda k, c: [(k0, c0) for k0, c0 in list(zip(k, c))[:count]]),
        topN=(
            'value', 'count',
            lambda k, c: [(k0, c0) for k0, c0 in sorted(zip(k, c), key=lambda i: i[1], reverse=True)[:count]],
        ),
    )
