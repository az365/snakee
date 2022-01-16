try:  # Assume we're a submodule in a package.
    from series import series_classes as sc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import series_classes as sc


def test_simple_smooth():
    data = [2, 5, 2], [2, 5, 2, 8, 5]
    expected = [[2, 3, 2], [2, 2, 2], [2, 3, 5, 5, 5], [2, 2, 6.5, 3.5, 5]]
    received = list()
    for d in data:
        for c in (False, True):
            received.append(
                sc.NumericSeries(d).smooth_simple_linear(3, exclude_center=c).get_list(),
            )
    assert received == expected


def test_get_nearest_date():
    data = {'2020-01-01': 10, '2021-01-01': 20}
    cases = ['2019-12-01', '2020-02-01', '2020-12-01', '2021-12-02']
    expected = ['2020-01-01', '2020-01-01', '2021-01-01', '2021-01-01']
    received = [sc.DateNumericSeries.from_dict(data).get_nearest_date(d) for d in cases]
    assert received == expected


def test_get_distance_for_nearest_date():
    data = {'2020-01-01': 10, '2021-01-01': 20}
    cases = ['2019-12-01', '2020-02-01', '2020-12-01', '2021-12-02']
    expected = [31, 31, 31, 335]
    received = [sc.DateNumericSeries.from_dict(data).get_distance_for_nearest_date(c) for c in cases]
    assert received == expected


def test_get_segment_for_date():
    data = {'2020-01-01': 10, '2021-01-01': 20, '2022-01-01': 30}
    cases = ['2019-12-01', '2020-02-01', '2021-02-01']
    expected = [
        [('2020-01-01', 10)],
        [('2020-01-01', 10), ('2021-01-01', 20)],
        [('2021-01-01', 20), ('2022-01-01', 30)],
    ]
    received = [sc.DateNumericSeries.from_dict(data).get_segment(d).get_list() for d in cases]
    assert received == expected


def test_get_interpolated_value():
    data = {'2019-01-01': 375, '2020-01-01': 10}
    cases = ['2018-12-01', '2019-02-01', '2019-12-01', '2020-12-02']
    expected = [375, 344, 41, 10]
    received = [sc.DateNumericSeries.from_dict(data).get_interpolated_value(d) for d in cases]
    assert received == expected


def test_interpolate():
    weight_benchmark = [('2020-{:02}-01'.format(m), m) for m in range(1, 13)] + [('2021-01-01', 13)]
    data = ['2020-01-01', '2021-01-01', '2022-01-01'], [10, 130, 10], ['2021-04-01', '2021-07-01', '2021-10-01']
    cases = (
        dict(how='linear'),
        dict(how='spline'),
        dict(how='weighted', weight_benchmark=sc.DateNumericSeries.from_items(weight_benchmark)),
    )
    expected = [[100, 70, 40], [100, 70, 40], [10, 7, 4]]
    received = []
    for c in cases:
        series = sc.DateNumericSeries(*data[0:2])
        interpolation = series.interpolate(data[2], **c)
        received += [interpolation.map_values(int).get_values()]
    assert expected == received


def test_find_base_date():
    data = ['2019-01-01', '2019-06-01', '2020-01-01', '2021-01-01'], [1, 1, 1, 1]
    cases = [
        '2017-02-01', '2018-02-01', '2018-02-01', '2019-02-01',
        '2019-05-15', '2020-02-01', '2021-02-01', '2022-02-01',
    ]
    expected = [
        ('2019-02-01', -2), ('2019-02-01', -1), ('2019-02-01', -1), ('2019-02-01', 0),
        ('2019-05-15', 0), ('2020-02-01', 0), ('2020-02-01', 1), ('2020-02-01', 2),
    ]
    received = [sc.DateNumericSeries(*data).find_base_date(d, 31, True) for d in cases]
    assert received == expected


def main():
    test_simple_smooth()
    test_get_nearest_date()
    test_get_distance_for_nearest_date()
    test_get_segment_for_date()
    test_get_interpolated_value()
    test_interpolate()
    test_find_base_date()


if __name__ == '__main__':
    main()
