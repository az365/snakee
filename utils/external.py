from typing import NoReturn
import warnings

try:  # Assume NumPy installed
    import numpy as np
except ImportError:
    np = None

try:  # Assume SciPy installed
    import scipy as sp
    from scipy import stats
    from scipy import interpolate
except ImportError:
    sp = None
    stats = None
    interpolate = None

try:  # Assume Pandas installed
    import pandas as pd
except ImportError:
    pd = None

try:  # Assume MatPlotLib installed
    from matplotlib import (
        pyplot as plt,
        patches as mp,
    )
except ImportError:
    plt = None
    mp = None

try:  # Assume psycopg2 installed
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

try:  # Assume boto3 installed
    import boto3
    import botocore.client as boto_core_client
except ImportError:
    boto3 = None
    boto_core_client = None


try:  # Assume IPython installed
    from IPython.core.display import display, clear_output, Markdown, HTML
except ImportError:
    display, clear_output, Markdown, HTML = print, None, None, None


class FallbackFake:
    library_name = None
    raise_warnings = True

    def __init__(self, *args, **kwargs):
        if self.raise_warnings:
            warnings.warn('{} library not installed and not imported'.format(self.library_name or 'Called'))


class FallbackDataframe(FallbackFake):
    library_name = 'pandas'


_use_objects_for_output = True
DataFrame = pd.DataFrame if pd else FallbackDataframe  # will be reset in set_use_objects_for_output()


def get_use_objects_for_output() -> bool:
    global _use_objects_for_output
    return _use_objects_for_output


def set_use_objects_for_output(use_objects_for_output: bool):
    global _use_objects_for_output
    global DataFrame
    _use_objects_for_output = use_objects_for_output
    if pd and use_objects_for_output:
        DataFrame = pd.DataFrame
    else:
        DataFrame = FallbackDataframe


def set_running_from_jupyter():
    set_use_objects_for_output(True)


def set_running_from_command_line():
    set_use_objects_for_output(False)


def raise_import_error(lib=None) -> NoReturn:
    if lib:
        raise ImportError('{} not installed'.format(lib))
    else:
        raise ImportError


set_use_objects_for_output(_use_objects_for_output)
