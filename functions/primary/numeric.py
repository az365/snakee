import math
from typing import Optional, Union, Iterable

try:  # Assume we're a sub-module in a package.
    from utils.external import (
        np, sp, pd, plt, interpolate,
        DataFrame,
        get_use_objects_for_output,
        raise_import_error,
    )
except ImportError:
    from ...utils.external import (
        np, sp, pd, plt, interpolate,
        DataFrame,
        get_use_objects_for_output,
        raise_import_error,
    )

if np:
    OptionalFloat = Union[float, np.ndarray, None]
    NUMERIC_TYPES = (int, float, np.number)
else:
    OptionalFloat = Optional[float]
    NUMERIC_TYPES = (int, float)

_min = min
_max = max
_sum = sum


def is_none(value) -> bool:
    if value is None:
        return True
    elif math.isnan(value):
        return True
    elif np:
        return value is np.nan
    else:
        return False


def is_defined(value) -> bool:
    return not is_none(value)


def is_nonzero(value) -> bool:
    if is_defined(value):
        return value != 0


def is_numeric(value) -> bool:
    return isinstance(value, NUMERIC_TYPES) and not isinstance(value, bool)


def filter_numeric(a: Iterable) -> list:
    return [i for i in a if is_numeric(i)]


def diff(c, v, take_abs=False, default=None) -> OptionalFloat:
    if c is None or v is None:
        return default
    else:
        result = v - c
        if take_abs:
            return abs(result)
        else:
            return result


def div(x: float, y: float, default=None) -> Optional[float]:
    if y:
        return (x or 0) / y
    else:
        return default


def median(a: Iterable, ignore_import_error: bool = False, safe: bool = True) -> OptionalFloat:
    if safe:
        a = filter_numeric(a)
    if np:
        return float(np.median(a))
    elif not ignore_import_error:
        raise_import_error('numpy')


def avg(a: Iterable, default=None, safe: bool = True, use_numpy: bool = True) -> OptionalFloat:
    if safe:
        a = filter_numeric(a)
    if a:
        if np and use_numpy:
            return float(np.mean(a))
        else:
            return div(sum(a), len(a), default=default)
    else:
        return default


def mean(a: Iterable, default=None, safe: bool = True, use_numpy: bool = True) -> OptionalFloat:
    return avg(a, default=default, safe=safe, use_numpy=use_numpy)  # alias


def min(a: Iterable, default=None, safe: bool = True) -> OptionalFloat:
    if safe:
        a = filter_numeric(a)
    if a:
        return _min(a)
    else:
        return default


def max(a: Iterable, default=None, safe: bool = True) -> OptionalFloat:
    if safe:
        a = filter_numeric(a)
    if a:
        return _max(a)
    else:
        return default


def sum(a: Iterable, default=None, safe: bool = True) -> OptionalFloat:
    if safe:
        a = filter_numeric(a)
    if a:
        return _sum(a)
    else:
        return default


def sqrt(value: float, default=None) -> OptionalFloat:
    if value is not None:
        if np:
            return np.sqrt(value)
        else:
            return math.sqrt(value)
    else:
        return default


def is_local_extremum(x_left, x_center, x_right, local_max=True, local_min=True) -> bool:
    result = False
    if local_max:
        result = x_center > x_left and x_center >= x_right
    if local_min:
        result = result or (x_center < x_left and x_center <= x_right)
    return result


def corr(a, b, ignore_import_error=False) -> float:
    if np:
        return float(np.corrcoef(a, b)[0, 1])
    elif not ignore_import_error:
        raise_import_error('numpy')


def spline_interpolate(x, y, ignore_import_error=False):
    if sp:
        assert len(x) == len(y)
        try:
            return interpolate.interp1d(x, y, kind='cubic')
        except ValueError:
            return interpolate.interp1d(x, y, kind='linear')
    elif not ignore_import_error:
        raise_import_error('scipy')


def get_dataframe(*args, ignore_import_error=False, **kwargs) -> DataFrame:
    if pd:
        return pd.DataFrame(*args, **kwargs)
    elif not ignore_import_error:
        raise_import_error('pandas')


def plot(*args, ignore_import_error=False, **kwargs):
    if plt:
        kwargs.pop('fmt')
        plt.plot(*args, **kwargs)
    elif not ignore_import_error:
        raise_import_error('matplotlib')


def plot_dates(*args, ignore_import_error=False, **kwargs):
    if plt:
        kwargs.pop('fmt')
        plt.plot_date(*args, **kwargs)
    elif not ignore_import_error:
        raise_import_error('matplotlib')