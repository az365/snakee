try:  # Assume we're a submodule in a package.
    from streams import stream_classes as sm
    from functions.primary import grouping as gr
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams import stream_classes as sm
    from ..primary import grouping as gr


def test_calc_histogram():
    example = [
        '1\t2\t3',
        '1\t4\t5',
        '1\t6\t7',
        '9\t4\t9',
    ]
    expected = [
        ('x', {1: 3, 9: 1}),
        ('y', {2: 1, 4: 2, 6: 1})
    ]
    received = sm.RegularStream(
        example,
        item_type=sm.ItemType.Line,
    ).to_row_stream(
        '\t',
    ).to_records(
        columns=('x', 'y', 'z'),
    ).select(
        '*',
        x=('x', int),
        y=('y', int),
        z=('z', int),
    ).apply_to_data(
        lambda a: gr.get_histograms(a, fields=['x', 'y']),
        item_type=sm.ItemType.Row,
    ).get_list()
    assert received == expected, f'{received} vs {expected}'


def test_sum_by_keys():
    example = [
        {'a': 1, 'b': 2, 'h': 1},
        {'a': 3, 'b': 4, 'h': 5},
        {'a': 1, 'b': 2, 'h': 2},
    ]
    expected = [((2, 1), {'h': 3}), ((4, 3), {'h': 5})]
    received = sm.RegularStream(
        example,
    ).apply_to_data(
        lambda a: gr.sum_by_keys(
            a,
            keys=('b', 'a'),
            counters=('h', ),
        ),
    ).get_list()
    assert received == expected, f'{received} vs {expected}'


def test_get_first_values():
    fields = ['a', 'b', 'c', 'd']
    example = [{'a': 1, 'b': 2}, {'b': 3, 'c': 4}]
    expected = {'a': 1, 'b': 2, 'c': 4}
    received = gr.get_first_values(
        example,
        fields,
    )
    assert received == expected, f'{received} vs {expected}'


def main():
    test_calc_histogram()
    test_sum_by_keys()
    test_get_first_values()


if __name__ == '__main__':
    main()
