try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO

FILL_CHAR = ' '
CROP_SUFFIX = '..'
SHORT_CROP_SUFFIX = '_'
LIST_DELIMITER = ', '
COLUMN_DELIMITER = ' '
TITLE_PREFIX = '==='

DEFAULT_STR = '-'
DEFAULT_TRUE_STR = 'Yes'
DEFAULT_FALSE_STR = 'No'
FALSE_VALUES = 'false', 'no', '-', '0', '0.0', ''
DICT_VALID_SIGN = {'True': '-', 'False': 'x', 'None': '-', AUTO.get_value(): '~'}

DEFAULT_PRECISION = 3
DEFAULT_LEN = 23

TAB_SYMBOL, TAB_SUBSTITUTE = '\t', ' -> '
PARAGRAPH_SYMBOL, PARAGRAPH_SUBSTITUTE = '\t', ' \\n '
