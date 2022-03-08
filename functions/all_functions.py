from warnings import warn

try:  # Assume we're a submodule in a package.
    from functions.secondary.all_secondary_fnctions import *
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .secondary.all_secondary_functions import *

MSG = 'functions.all_functions module is deprecated, use functions.secondary.all_secondary_functions instead'
warn(MSG, stacklevel=2)
