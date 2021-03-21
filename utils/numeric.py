import math

try:
    import numpy as np
except ImportError:
    np = None
try:
    from scipy import interpolate
except ImportError:
    interpolate = None
try:
    import pandas as pd
except ImportError:
    pd = None
try:
    from matplotlib import pyplot as plt
except ImportError:
    plt = None


def is_none(value):
    if value is None:
        return True
    elif math.isnan(value):
        return True
    elif np:
        return value is np.nan
    else:
        return False


def is_defined(value):
    return not is_none(value)


def is_nonzero(value):
    if is_defined(value):
        return value != 0


def div(x, y, default=None):
    if y:
        return (x or 0) / y
    else:
        return default


def mean(a, default=None):
    if a:
        if np:
            return np.mean(a)
        else:
            return div(sum(a), len(a), default=default)
    else:
        return default


def sqrt(value, default=None):
    if value:
        if np:
            return np.sqrt(value)
        else:
            return math.sqrt(value)
    else:
        return default


def diff(c, v, take_abs=False):
    result = v - c
    if take_abs:
        return abs(result)
    else:
        return result


def is_local_extremum(x_left, x_center, x_right, local_max=True, local_min=True):
    result = False
    if local_max:
        result = x_center > x_left and x_center >= x_right
    if local_min:
        result = result or (x_center < x_left and x_center <= x_right)
    return result


def _raise_import_error(lib=None):
    if lib:
        raise ImportError('{} not installed'.format(lib))
    else:
        raise ImportError


def median(a, ignore_import_error=False):
    if np:
        return np.median(a)
    elif not ignore_import_error:
        _raise_import_error('numpy')


def corr(a, b, ignore_import_error=False):
    if np:
        return np.corrcoef(a, b)[0, 1]
    elif not ignore_import_error:
        _raise_import_error('numpy')


def spline_interpolate(x, y, ignore_import_error=False):
    if interpolate:
        assert len(x) == len(y)
        try:
            return interpolate.interp1d(x, y, kind='cubic')
        except ValueError:
            return interpolate.interp1d(x, y, kind='linear')
    elif not ignore_import_error:
        _raise_import_error('scipy')


def get_dataframe(*args, ignore_import_error=False, **kwargs):
    if pd:
        return pd.DataFrame(*args, **kwargs)
    elif not ignore_import_error:
        _raise_import_error('pandas')


def plot(*args, ignore_import_error=False, **kwargs):
    if plt:
        kwargs.pop('fmt')
        plt.plot(*args, **kwargs)
    elif not ignore_import_error:
        _raise_import_error('matplotlib')


def plot_dates(*args, ignore_import_error=False, **kwargs):
    if plt:
        kwargs.pop('fmt')
        plt.plot_date(*args, **kwargs)
    elif not ignore_import_error:
        _raise_import_error('matplotlib')
