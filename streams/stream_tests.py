try:  # Assume we're a submodule in a package.
    from functions.secondary import all_secondary_functions as fs
    from streams import stream_classes as sm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..functions.secondary import all_secondary_functions as fs
    from . import stream_classes as sm


EXAMPLE_FILENAME = 'test_file.tmp'
EXAMPLE_INT_SEQUENCE = [1, 3, 5, 7, 9, 2, 4, 6, 8]
EXAMPLE_CSV_ROWS = [
    'a,1',
    'b,"2,22"',
    'c,3',
]


def test_map():
    expected_types = ['Any', 'Line', 'Line', 'Line']
    received_types = list()
    expected_0 = [-i for i in EXAMPLE_INT_SEQUENCE]
    received_0 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).to_iter(
    ).map(
        lambda i: -i,
    ).submit(
        received_types,
        lambda f: f.get_item_type().get_name(),
    ).get_list()
    assert received_0 == expected_0, 'test case 0'
    expected_1 = [str(-i) for i in EXAMPLE_INT_SEQUENCE]
    received_1 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).map_to_type(
        lambda i: str(-i),
        stream_type=sm.LineStream,
    ).submit(
        received_types,
        lambda f: f.get_item_type().get_name(),
    ).get_list()
    assert received_1 == expected_1, f'test case 1: {received_1} != {expected_1}'
    expected_2 = [str(-i) for i in EXAMPLE_INT_SEQUENCE]
    received_2 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).map_to_type(
        lambda i: str(-i),
        stream_type=sm.StreamType.LineStream,
    ).submit(
        received_types,
        lambda f: f.get_item_type().get_name(),
    ).get_list()
    assert received_2 == expected_2, f'test case 2: {received_2} != {expected_2}'
    expected_3 = [str(-i) for i in EXAMPLE_INT_SEQUENCE]
    received_3 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).map_to_type(
        lambda i: str(-i),
        stream_type='LineStream',
    ).submit(
        received_types,
        lambda f: f.get_item_type().get_name(),
    ).get_list()
    assert received_3 == expected_3, f'test case 3: {received_3} != {expected_3}'
    assert received_types == expected_types, f'test for types: {received_types} != {expected_types}'


def test_flat_map():
    expected = ['a', 'a', 'b', 'b']
    received = sm.AnyStream(
        ['a', 'b']
    ).flat_map(
        lambda i: [i, i],
    ).get_list()
    assert received == expected


def test_filter():
    expected = [7, 6, 8]
    received = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).filter(
        lambda i: i > 5,
        lambda i: i <= 8,
    ).get_list()
    assert received == expected


def test_records_filter():
    example = [
        dict(a=11, b=12),
        dict(a=21, b=22),
        dict(a=21, b=32),
        dict(a=41, b=42),
    ]
    expected = example[2:3]
    received = sm.RecordStream(
        example,
    ).filter(
        a=21,
        b=lambda x: x >= 30,
    ).get_list()
    assert received == expected


def test_take():
    expected = [1, 3, 5, 7, 9]
    received = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).take(
        5,
    ).get_list()
    assert received == expected


def test_skip():
    expected = [2, 4, 6, 8]
    received = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).skip(
        5,
    ).get_list()
    assert received == expected


def test_map_filter_take():
    expected = [-1, -3, -5]
    received = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).map(
        lambda i: -i,
    ).filter(
        lambda i: i % 2,
    ).take(
        3,
    ).get_list()
    assert received == expected


def test_any_select():
    example = ['12', '123', '1234']
    expected_1 = [
        (2, 12.0, '12'),
        (3, 123.0, '123'),
        (4, 1234.0, '1234'),
    ]
    received_1 = sm.AnyStream(
        example,
    ).select(
        len,
        float,
        str,
    ).get_list()
    assert received_1 == expected_1, 'test case 1: AnyStream to RowStream'
    expected_2 = [
        {'a': 2, 'b': 2.0, 'c': '12'},
        {'a': 3, 'b': 3.0, 'c': '123'},
        {'a': 4, 'b': 4.0, 'c': '1234'},
    ]
    received_2 = sm.AnyStream(
        example,
    ).select(
        a=len,
        b=lambda i: float(len(i)),
        c=(str, ),
    ).get_list()
    assert received_2 == expected_2, 'test case 1: AnyStream to RowStream'


def test_records_select():
    expected_1 = [
        {'a': '1', 'd': None, 'e': None, 'f': '11', 'g': None, 'h': None},
        {'a': None, 'd': '2,22', 'e': None, 'f': 'NoneNone', 'g': None, 'h': None},
        {'a': None, 'd': None, 'e': '3', 'f': 'NoneNone', 'g': '3', 'h': '3'},
    ]
    received_1 = sm.AnyStream(
        EXAMPLE_CSV_ROWS,
    ).to_line_stream(
    ).to_row_stream(
        delimiter=',',
    ).map_to_records(
        lambda p: {fs.first()(p): fs.second()(p)},
    ).select(
        'a',
        h='g',
        g='e',
        d='b',
        e=lambda r: r.get('c'),
        f=('a', lambda v: str(v)*2),
    ).get_list()
    assert received_1 == expected_1, f'test case 1, records: {received_1} != {expected_1}'
    expected_2 = [
        (1.00, 'a', '1', 'a'),
        (2.22, 'b', '2.22', 'b'),
        (3.00, 'c', '3', 'c'),
    ]
    received_2 = sm.AnyStream(
        EXAMPLE_CSV_ROWS,
    ).to_line_stream(
    ).to_row_stream(
        delimiter=',',
    ).select(
        0,
        lambda s: s[1].replace(',', '.'),
    ).select(
        (float, 1),
        '*',
        0,
    ).get_list()
    assert received_2 == expected_2, f'test case 2: rows {received_2} != {expected_2}'


def test_enumerated():
    expected = list(enumerate(EXAMPLE_INT_SEQUENCE))
    received = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).enumerate().get_list()
    assert received == expected


def test_add():
    addition = list(reversed(EXAMPLE_INT_SEQUENCE))
    expected_1 = EXAMPLE_INT_SEQUENCE + addition
    expected_2 = addition + EXAMPLE_INT_SEQUENCE
    received_1i = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition
    ).get_list()
    assert received_1i == expected_1, 'test case 1i'
    received_2i = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition,
        before=True,
    ).get_list()
    assert received_2i == expected_2, 'test case 2i'
    received_1f = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        sm.AnyStream(addition),
    ).get_list()
    assert received_1f == expected_1, 'test case 1f'
    received_2f = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        sm.AnyStream(addition),
        before=True,
    ).get_list()
    assert received_2f == expected_2, 'test case 2f'


def test_add_records():
    addition = list(reversed(EXAMPLE_INT_SEQUENCE))
    expected_1 = list(map(lambda v: dict(item=v), EXAMPLE_INT_SEQUENCE + addition))
    expected_2 = list(map(lambda v: dict(item=v), addition + EXAMPLE_INT_SEQUENCE))
    received_1 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).map_to_records(
        lambda i: dict(item=i),
    ).add(
        sm.AnyStream(addition).to_record_stream(),
    ).get_list()
    assert received_1 == expected_1, f'test case 1i: {received_1} != {expected_1}'
    received_2 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).to_record_stream(
    ).add(
        sm.AnyStream(addition).to_record_stream(),
        before=True,
    ).get_list()
    assert received_2 == expected_2, f'test case 2i: {received_2} != {expected_2}'


def test_separate_first():
    expected = [EXAMPLE_INT_SEQUENCE[0], EXAMPLE_INT_SEQUENCE[1:]]
    received = list(
        sm.AnyStream(
            EXAMPLE_INT_SEQUENCE,
        ).separate_first()
    )
    received[1] = received[1].get_list()
    assert received == expected


def test_split_by_pos():
    pos_1, pos_2 = 3, 5
    expected_1 = EXAMPLE_INT_SEQUENCE[:pos_1], EXAMPLE_INT_SEQUENCE[pos_1:]
    a, b = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).split(
        pos_1,
    )
    received_1 = a.get_list(), b.get_list()
    assert received_1 == expected_1, f'test case 1, {received_1} != {expected_1}'
    expected_2 = (
        [pos_1] + EXAMPLE_INT_SEQUENCE[:pos_1],
        [pos_2 - pos_1] + EXAMPLE_INT_SEQUENCE[pos_1:pos_2],
        [len(EXAMPLE_INT_SEQUENCE) - pos_2] + EXAMPLE_INT_SEQUENCE[pos_2:],
    )
    a, b, c = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).split(
        (pos_1, pos_2),
    )
    received_2 = a.count_to_items().get_list(), b.count_to_items().get_list(), c.count_to_items().get_list()
    assert received_2 == expected_2, 'test case 2'


def test_split_by_func():
    expected = [1, 3, 2, 4], [5, 7, 9, 6, 8]
    a, b = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE
    ).split(
        lambda i: i >= 5,
    )
    received = a.get_list(), b.get_list()
    assert received == expected


def test_split_by_step():
    expected = [
        [1, 3, 5, 7],
        [9, 2, 4, 6],
        [8],
    ]
    split_0 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE
    ).split_to_disk_by_step(
        step=4,
    )
    received_0 = [f.get_list() for f in split_0]
    assert received_0 == expected, 'test case 0'
    split_1 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE
    ).split_to_iter_by_step(
        step=4,
    )
    received_1 = [f.get_list() for f in split_1]
    assert received_1 == expected, 'test case 1'


def test_memory_sort():
    expected = [7, 9, 8, 6, 5, 4, 3, 2, 1]
    received = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).memory_sort(
        key=lambda i: 777 if i == 7 else i,
        reverse=True,
    ).get_list()
    assert received == expected


def test_disk_sort_by_key():
    expected = [[k, str(k) * k] for k in range(1, 10)]
    received = sm.AnyStream(
        [(k, str(k) * k) for k in EXAMPLE_INT_SEQUENCE],
    ).to_pairs(
    ).disk_sort(
        fs.first(),  # KEY
        step=5,
    ).get_list()
    assert received == expected


def test_sort():
    expected_0 = list(reversed(range(1, 10)))
    received_0 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).set_meta(
        max_items_in_memory=4,
    ).sort(
        reverse=True,
    ).get_list()
    assert received_0 == expected_0, 'test case 0'
    expected_1 = list(reversed(range(1, 10)))
    received_1 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).sort(
        lambda i: -i,
        reverse=False,
        step=4,
    ).get_list()
    assert received_1 == expected_1, 'test case 1'
    expected_2 = list(reversed(range(1, 10)))
    received_2 = sm.AnyStream(
        EXAMPLE_INT_SEQUENCE,
    ).sort(
        lambda i: 100,
        lambda i: -i,
        lambda i: i,
        reverse=False,
        step=4,
    ).get_list()
    assert received_2 == expected_2, 'test case 2'


def test_sorted_group_by_key():
    example = [
        (1, 11), (1, 12),
        (2, 21),
        (3, 31), (3, 32), (3, 33),
    ]
    expected = [
        (1, [11, 12]),
        (2, [21]),
        (3, [31, 32, 33]),
    ]
    received = sm.AnyStream(
        example
    ).to_pairs(
    ).sorted_group_by(
        0,
        values=[1],
        as_pairs=True,
    ).get_list()
    assert received == expected, f'{received} != {expected}'


def test_group_by():
    example = [
        (1, 11), (1, 12),
        (2, 21),
        (3, 31), (3, 32), (3, 33),
    ]
    expected = [
        [11, 12],
        [21],
        [31, 32, 33],
    ]
    received_0 = sm.AnyStream(example).to_row_stream().to_record_stream(
        columns=('x', 'y'),
    ).group_by(
        'x',
        as_pairs=True,
    ).map_to_type(
        lambda a: [i.get('y') for i in a[1]],
        stream_type=sm.StreamType.RowStream,
    ).get_list()
    assert received_0 == expected, f'test case 0: {received_0} != {expected}'

    received_1 = sm.AnyStream(example).to_row_stream().to_record_stream(
        columns=('x', 'y'),
    ).group_by(
        'x',
        as_pairs=False,
    ).map_to_type(
        lambda a: [i.get('y') for i in a],
        stream_type=sm.StreamType.RowStream,
    ).get_list()
    assert received_1 == expected, f'test case 1: {received_1} != {expected}'


def test_any_join():
    example_a = ['a', 'b', 1]
    example_b = ['c', 2, 33]
    expected_0 = [('a', 'c'), ('b', 'c'), (1, 33)]
    received_0 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key=type,
        right_is_uniq=True,
    ).get_list()
    assert received_0 == expected_0, 'test case 0: right is uniq'
    expected_1 = [('a', 'c'), ('b', 'c'), (1, 2), (1, 33)]
    received_1 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key=type,
        right_is_uniq=False,
    ).get_list()
    assert set(received_1) == set(expected_1), 'test case 1: right is not uniq'
    expected_2 = [('a', 'c'), ('b', 'c'), (1, 2)]
    received_2 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key=(type, lambda i: len(str(i))),
        how='left',
        right_is_uniq=False,
    ).get_list()
    assert set(received_2) == set(expected_2), 'test case 2: left join using composite key'
    expected_3 = [('a', 'c'), ('b', 'c'), (1, 2), (None, 33)]
    received_3 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key=(type, lambda i: len(str(i))),
        how='full',
        right_is_uniq=False,
    ).get_list()
    assert set(received_3) == set(expected_3), 'test case 3: full join using composite key'
    expected_4 = [(1, 2), ('a', 'c'), ('b', 'c')]
    received_4 = sm.AnyStream(
        example_a,
    ).join(
        sm.AnyStream(example_b),
        key=(lambda i: str(type(i)), lambda i: len(str(i))),
        how='inner',
    ).get_list()
    assert set(received_4) == set(expected_4), 'test case 4: sorted left join'
    expected_5 = [(1, 2), (None, 33), ('a', 'c'), ('b', 'c')]
    received_5 = sm.AnyStream(
        example_a,
    ).join(
        sm.AnyStream(example_b),
        key=(lambda i: str(type(i)), lambda i: len(str(i))),
        how='right',
    ).get_list()
    assert set(received_5) == set(expected_5), 'test case 5: sorted right join'


def test_records_join():
    example_a = [{'x': 0, 'y': 0, 'z': 0}, {'y': 2, 'z': 7}, {'x': 8, 'y': 9}]
    example_b = [{'x': 1, 'y': 2, 'z': 3}, {'x': 4, 'y': 2}, {'x': 6, 'y': 0}]
    expected_0 = [{'x': 6, 'y': 0, 'z': 0}, {'x': 4, 'y': 2, 'z': 7}, {'x': 8, 'y': 9}]
    received_0 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key='y',
        right_is_uniq=True,
    ).get_list()
    assert received_0 == expected_0, f'test case 0: right is uniq {received_0} != {expected_0}'
    expected_1 = [{'x': 6, 'y': 0, 'z': 0}, {'x': 1, 'y': 2, 'z': 3}, {'x': 4, 'y': 2, 'z': 7}, {'x': 8, 'y': 9}]
    received_1 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key='y',
        right_is_uniq=False,
    ).get_list()
    assert received_1 == expected_1, f'test case 1: right is not uniq {received_1} != {expected_1}'
    expected_2 = [{'x': 6, 'y': 0, 'z': 0}, {'x': 1, 'y': 2, 'z': 3}, {'x': 4, 'y': 2, 'z': 7}, {'x': 8, 'y': 9}]
    received_2 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key='y',
        how='left',
        right_is_uniq=False,
    ).get_list()
    assert received_2 == expected_2, f'test case 2: left join, {received_2} != {expected_2}'
    expected_3 = [{'x': 6, 'y': 0, 'z': 0}, {'x': 1, 'y': 2, 'z': 3}, {'x': 4, 'y': 2, 'z': 7}, {'x': 8, 'y': 9}]
    received_3 = sm.AnyStream(
        example_a,
    ).map_side_join(
        sm.AnyStream(example_b),
        key='y',
        how='full',
        right_is_uniq=False,
    ).get_list()
    assert received_3 == expected_3, f'test case 3: full join {received_3} != {expected_3}'
    expected_4 = [{'x': 6, 'y': 0, 'z': 0}, {'x': 1, 'y': 2, 'z': 3}, {'x': 4, 'y': 2, 'z': 7}]
    received_4 = sm.RecordStream(
        example_a,
    ).join(
        sm.AnyStream(example_b),
        key='y',
        how='inner',
    ).get_list()
    assert received_4 == expected_4, f'test case 4: sorted left join {received_4} != {expected_4}'
    expected_5 = [{'x': 6, 'y': 0, 'z': 0}, {'x': 1, 'y': 2, 'z': 3}, {'x': 4, 'y': 2, 'z': 7}]
    received_5 = sm.RecordStream(
        example_a,
    ).join(
        sm.AnyStream(example_b),
        key='y',
        how='right',
    ).get_list()
    assert received_5 == expected_5, f'test case 5: sorted right join {received_5} != {expected_5}'


def test_to_rows():
    expected = [['a', '1'], ['b', '2,22'], ['c', '3']]
    received = sm.AnyStream(
        EXAMPLE_CSV_ROWS,
    ).to_line_stream(
    ).to_row_stream(
        ',',
    ).get_list()
    assert received == expected, f'{received} != {expected}'


def test_parse_json():
    example = ['{"a": "b"}', 'abc', '{"d": "e"}']
    expected = [dict(a='b'), dict(_err='JSONDecodeError'), dict(d='e')]
    received = sm.AnyStream(
        example,
    ).to_line_stream(
    ).to_record_stream(
    ).get_list()
    assert received == expected, f'{received} != {expected}'


def test_unfold():
    example_records = [
        dict(key='a', value=[11, 12, 13]),
        dict(key='b', value=[21, 22, 23]),
    ]
    expected_records = [
        dict(key='a', value=11), dict(key='a', value=12), dict(key='a', value=13),
        dict(key='b', value=21), dict(key='b', value=22), dict(key='b', value=23),
    ]
    expected_rows = [
        ['a', 11], ['a', 12], ['a', 13],
        ['b', 21], ['b', 22], ['b', 23],
    ]
    received_records = sm.RecordStream(
        example_records,
    ).flat_map(
        fs.unfold_lists('value', number_field=None),
    ).get_list()
    assert received_records == expected_records, f'test case 1: records {received_records} != {expected_records}'
    received_rows = sm.RecordStream(
        example_records,
    ).to_row_stream(
        columns=['key', 'value'],
    ).flat_map(
        fs.unfold_lists(1, number_field=None),
    ).get_list()
    assert received_rows == expected_rows, f'test case 2: rows {received_rows} != {expected_rows}'


def smoke_test_show():
    stream0 = sm.AnyStream(
        EXAMPLE_CSV_ROWS,
    ).to_line_stream(
    ).to_row_stream(
        delimiter=',',
    ).map_to_records(
        lambda p: {fs.first()(p): fs.second()(p)},
    ).select(
        'a',
        h='g',
        g='e',
        d='b',
        e=lambda r: r.get('c'),
        f=('a', lambda v: str(v)*2),
    )
    stream0.show()
    stream0.collect().show()


def main():
    test_map()
    test_flat_map()
    test_filter()
    test_records_filter()
    test_take()
    test_skip()
    test_map_filter_take()
    test_any_select()
    test_records_select()
    test_enumerated()
    test_add()
    test_add_records()
    test_separate_first()
    test_split_by_pos()
    test_split_by_func()
    test_split_by_step()
    test_memory_sort()
    test_disk_sort_by_key()
    test_sort()
    test_sorted_group_by_key()
    test_group_by()
    test_any_join()
    test_records_join()
    test_to_rows()
    test_parse_json()
    test_unfold()
    smoke_test_show()


if __name__ == '__main__':
    main()
