from typing import Optional
import inspect

try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.classes.enum import ClassType


class SeriesType(ClassType):
    AnySeries = 'AnySeries'
    SortedSeries = 'SortedSeries'
    DateSeries = 'DateSeries'
    NumericSeries = 'NumericSeries'
    SortedNumericSeries = 'SortedNumericSeries'
    KeyValueSeries = 'KeyValueSeries'
    SortedKeyValueSeries = 'SortedKeyValueSeries'
    SortedNumericKeyValueSeries = 'SortedNumericKeyValueSeries'
    DateNumericSeries = 'DateNumericSeries'

    @classmethod
    def detect(cls, obj, default=None) -> ClassType:
        if isinstance(obj, SeriesType):
            return obj
        elif isinstance(obj, str):
            name = obj
        elif inspect.isclass(obj):
            name = obj.__name__
        elif default:
            return default
        else:
            raise ValueError(f'SeriesType for {obj} not detected')
        return SeriesType(name)

    @classmethod
    def of(cls, obj):
        if isinstance(obj, SeriesType):
            return obj
        elif isinstance(obj, str):
            return SeriesType(obj)
        else:
            return cls.detect(obj)

    def isinstance(self, series, by_type: bool = True) -> Optional[bool]:
        if hasattr(series, 'get_series_type'):
            return series.get_series_type() == self
        else:
            return super().isinstance(series, by_type=by_type)


SeriesType.prepare()
