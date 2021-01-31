try:  # Assume we're a sub-module in a package.
    from series.abstract_series import AbstractSeries
    from series.simple.any_series import AnySeries
    from series.simple.numeric_series import NumericSeries
    from series.simple.sorted_series import SortedSeries
    from series.pairs.sorted_numeric_series import SortedNumericSeries
    from series.simple.date_series import DateSeries
    from series.pairs.key_value_series import KeyValueSeries
    from series.pairs.sorted_key_value_series import SortedKeyValueSeries
    from series.pairs.sorted_numeric_key_value_series import SortedNumericKeyValueSeries
    from series.pairs.date_numeric_series import DateNumericSeries
    from utils import (
        numeric as nm,
        dates as dt,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..series.abstract_series import AbstractSeries
    from ..series.simple.any_series import AnySeries
    from ..series.simple.numeric_series import NumericSeries
    from ..series.simple.sorted_series import SortedSeries
    from ..series.pairs.sorted_numeric_series import SortedNumericSeries
    from ..series.simple.date_series import DateSeries
    from ..series.pairs.key_value_series import KeyValueSeries
    from ..series.pairs.sorted_key_value_series import SortedKeyValueSeries
    from ..series.pairs.sorted_numeric_key_value_series import SortedNumericKeyValueSeries
    from ..series.pairs.date_numeric_series import DateNumericSeries
    from ..utils import (
        numeric as nm,
        dates as dt,
    )
