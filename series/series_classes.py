try:  # Assume we're a submodule in a package.
    from series.series_type import SeriesType
    from series.abstract_series import AbstractSeries
    from series.interfaces.any_series_interface import AnySeriesInterface
    from series.interfaces.sorted_series_interface import SortedSeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface
    from series.interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
    from series.interfaces.date_series_interface import DateSeriesInterface
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
    from series.interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface
    from series.interfaces.sorted_numeric_key_value_series_interface import SortedNumericKeyValueSeriesInterface
    from series.interfaces.date_numeric_series_interface import DateNumericSeriesInterface
    from series.simple.any_series import AnySeries
    from series.simple.numeric_series import NumericSeries
    from series.simple.sorted_series import SortedSeries
    from series.simple.sorted_numeric_series import SortedNumericSeries
    from series.simple.date_series import DateSeries
    from series.pairs.key_value_series import KeyValueSeries
    from series.pairs.sorted_key_value_series import SortedKeyValueSeries
    from series.pairs.sorted_numeric_key_value_series import SortedNumericKeyValueSeries
    from series.pairs.date_numeric_series import DateNumericSeries
    from functions.primary import numeric as nm, dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .series_type import SeriesType
    from .abstract_series import AbstractSeries
    from .interfaces.any_series_interface import AnySeriesInterface
    from .interfaces.sorted_series_interface import SortedSeriesInterface
    from .interfaces.numeric_series_interface import NumericSeriesInterface
    from .interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
    from .interfaces.date_series_interface import DateSeriesInterface
    from .interfaces.key_value_series_interface import KeyValueSeriesInterface
    from .interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface
    from .interfaces.sorted_numeric_key_value_series_interface import SortedNumericKeyValueSeriesInterface
    from .interfaces.date_numeric_series_interface import DateNumericSeriesInterface
    from .simple.any_series import AnySeries
    from .simple.numeric_series import NumericSeries
    from .simple.sorted_series import SortedSeries
    from .simple.sorted_numeric_series import SortedNumericSeries
    from .simple.date_series import DateSeries
    from .pairs.key_value_series import KeyValueSeries
    from .pairs.sorted_key_value_series import SortedKeyValueSeries
    from .pairs.sorted_numeric_key_value_series import SortedNumericKeyValueSeries
    from .pairs.date_numeric_series import DateNumericSeries
    from ..functions.primary import numeric as nm, dates as dt

DICT_SERIES_CLASSES = dict(
    AnySeries=AnySeries,
    SortedSeries=SortedSeries,
    DateSeries=DateSeries,
    NumericSeries=NumericSeries,
    SortedNumericSeries=SortedNumericSeries,
    KeyValueSeries=KeyValueSeries,
    SortedKeyValueSeries=SortedKeyValueSeries,
    SortedNumericKeyValueSeries=SortedNumericKeyValueSeries,
    DateNumericSeries=DateNumericSeries,
)

SeriesType.set_dict_classes(DICT_SERIES_CLASSES)
