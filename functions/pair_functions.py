from typing import Callable

try:  # Assume we're a sub-module in a package.
    from utils import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import numeric as nm


def shifted_func(func) -> Callable:
    def func_(x, y) -> list:
        assert len(x) == len(y)
        shift_max = len(x) - 1
        result = list()
        for shift in range(-shift_max + 2, 0):
            shifted_x = x[0: shift_max + shift]
            shifted_y = y[- shift: shift_max]
            stat = func(shifted_x, shifted_y)
            result.append(stat)
        for shift in range(0, shift_max - 1):
            shifted_x = x[shift: shift_max]
            shifted_y = y[0: shift_max - shift]
            stat = func(shifted_x, shifted_y)
            result.append(stat)
        return result
    return func_


def pair_filter(function=nm.is_defined) -> Callable:
    def func(a, b) -> tuple:
        a_filtered, b_filtered = list(), list()
        for cur_a, cur_b in zip(a, b):
            take_a = function(cur_a)
            take_b = function(cur_b)
            if take_a and take_b:
                a_filtered.append(cur_a)
                b_filtered.append(cur_b)
        return a_filtered, b_filtered
    return func


def pair_stat(stat_func, filter_func=None) -> Callable:
    def func(a, b) -> float:
        if filter_func:
            data = pair_filter(filter_func)(a, b)
        else:
            data = (a, b)
        return stat_func(*data)
    return func


def corr() -> Callable:
    return pair_stat(
        filter_func=nm.is_nonzero,
        stat_func=lambda *v: nm.corr(*v),
    )
