import numpy as np

try:  # Assume we're a sub-module in a package.
    from functions import basic_functions as bf
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import basic_functions as bf


def shifted_func(func):
    def func_(x, y):
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


def pair_filter(function=bf.not_none()):
    def func(a, b):
        a_filtered, b_filtered = list(), list()
        for cur_a, cur_b in zip(a, b):
            take_a = function(cur_a)
            take_b = function(cur_b)
            if take_a and take_b:
                a_filtered.append(cur_a)
                b_filtered.append(cur_b)
        return a_filtered, b_filtered
    return func


def pair_stat(stat_func, filter_func=None):
    def func(a, b):
        if filter_func:
            data = pair_filter(filter_func)(a, b)
        else:
            data = (a, b)
        return stat_func(*data)
    return func


def corr():
    return pair_stat(
        filter_func=bf.nonzero(),
        stat_func=lambda *v: np.corrcoef(*v)[0, 1],
    )
