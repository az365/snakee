from typing import Optional, Iterable, Sequence, Union
import math

try:  # Assume we're a submodule in a package.
    from base.classes.typing import (
        Numeric as DefaultNumeric,
        NUMERIC_TYPES as DEFAULT_NUMERIC_TYPES,
    )
    from utils.external import (
        np, sp, pd, plt, stats, interpolate,
        DataFrame,
        get_use_objects_for_output,
        raise_import_error,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import (
        Numeric as DefaultNumeric,
        NUMERIC_TYPES as DEFAULT_NUMERIC_TYPES,
    )
    from ...utils.external import (
        np, sp, pd, plt, stats, interpolate,
        DataFrame,
        get_use_objects_for_output,
        raise_import_error,
    )

if np:
    ExtFloat = Union[float, np.number, np.ndarray]
    OptionalFloat = Optional[ExtFloat]
    NumericTypes = Union[DefaultNumeric, np.number, np.ndarray]
    NUMERIC_TYPES = *DEFAULT_NUMERIC_TYPES, np.number
    MUTABLE = list, np.ndarray
    Mutable = Union[list, np.ndarray]
else:
    ExtFloat = float
    OptionalFloat = Optional[float]
    NumericTypes = DefaultNumeric
    NUMERIC_TYPES = DEFAULT_NUMERIC_TYPES
    MUTABLE = list
    Mutable = list

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


def sign(value: float, zero: int = 0, plus: int = 1, minus: int = -1) -> int:
    if not value:
        return zero
    elif value > 0:
        return plus
    else:
        return minus


def increment(c, v, take_abs=False, default=None) -> OptionalFloat:
    if c is None or v is None:
        return default
    else:
        result = v - c
        if take_abs:
            return abs(result)
        else:
            return result


def diff(v, c, take_abs=False, default=None) -> OptionalFloat:
    return increment(c, v, take_abs=take_abs, default=default)


def div(x: NumericTypes, y: NumericTypes, default: OptionalFloat = None) -> OptionalFloat:
    if y:
        return (x or 0) / y
    else:
        return default


def lift(a: NumericTypes, b: NumericTypes, take_abs: bool = False, default: OptionalFloat = None) -> OptionalFloat:
    if a is None or not b:
        return default
    else:
        result = (b - a) / a
        if take_abs:
            return abs(result)
        else:
            return result


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


def log(value: float, base: OptionalFloat = None, default=None) -> OptionalFloat:
    try:
        if base:
            return math.log(value, base)
        else:
            return math.log(value)
    except ValueError:
        return default


def round_py(value, ndigits: int = 0, exclude_negative: bool = False) -> NumericTypes:
    if value < 0 and exclude_negative:
        return 0
    if ndigits <= 0:
        value = int(value)
    return round(value, ndigits)


def round_to(value: NumericTypes, step: NumericTypes, exclude_negative: bool = False) -> NumericTypes:
    if not step:
        step = 1
    value_type = type(step)
    if value < 0 and exclude_negative:
        return 0
    elif step:
        value = int(value / step) * step
        return value_type(value)
    else:
        raise ZeroDivisionError('{} / {}'.format(value, step))


def is_local_extreme(x_left, x_center, x_right, local_max=True, local_min=True) -> bool:
    result = False
    if local_max:
        result = x_center > x_left and x_center >= x_right
    if local_min:
        result = result or (x_center < x_left and x_center <= x_right)
    return result


def var(a: Sequence, default: OptionalFloat = None) -> float:
    if a:
        return float(np.var(a))
    return default


def corr(a, b, ignore_import_error=False) -> float:
    if np:
        return float(np.corrcoef(a, b)[0, 1])
    elif not ignore_import_error:
        raise_import_error('numpy')


def t_test_1sample_p_value(series: Iterable, value: float = 0, ignore_import_error=False) -> float:
    if stats:
        t_test = stats.ttest_1samp(series, value)
        return t_test[1]
    elif not ignore_import_error:
        raise_import_error('scipy')


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


def plot(*args, ignore_import_error=False, **kwargs) -> None:
    if plt:
        kwargs.pop('fmt')
        plt.plot(*args, **kwargs)
    elif not ignore_import_error:
        raise_import_error('matplotlib')


def plot_dates(*args, ignore_import_error=False, **kwargs) -> None:
    if plt:
        kwargs.pop('fmt')
        plt.plot_date(*args, **kwargs)
    elif not ignore_import_error:
        raise_import_error('matplotlib')
