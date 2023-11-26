import re

try:  # Assume we're a submodule in a package.
    from base.constants.chars import (
        EMPTY, DEFAULT_STR,
        TAB_CHAR, REQUEST_DELIMITER, ITEMS_DELIMITER, SEMICOLON, COMMA, SPACE,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .chars import (
        EMPTY, DEFAULT_STR,
        TAB_CHAR, REQUEST_DELIMITER, ITEMS_DELIMITER, SEMICOLON, COMMA, SPACE,
    )

DEFAULT_ENCODING = 'utf8'

LONG_LINE_LEN = 900
JUPYTER_LINE_LEN = 120
DEFAULT_LINE_LEN = JUPYTER_LINE_LEN
SHORT_LINE_LEN = 30
EXAMPLE_STR_LEN = 12
DEFAULT_FLOAT_LEN = 12
DEFAULT_INT_LEN = 7

RE_LETTERS = re.compile('[^a-zа-я ]')
NORM_LETTER_PAIRS = [('ё', 'е'), ]

ZERO_VALUES = None, 'None', EMPTY, DEFAULT_STR, 0
DEFAULT_TRUE_STR = 'Yes'
DEFAULT_FALSE_STR = 'No'
FALSE_VALUES = 'false', 'no', '0', '0.0', DEFAULT_STR, EMPTY
STR_FALSE_SYNONYMS = ['False', 'None', 'none'] + list(FALSE_VALUES)  # 'false', 'no', '0', '0.0', DEFAULT_STR, EMPTY
POPULAR_DELIMITERS = TAB_CHAR, REQUEST_DELIMITER, ITEMS_DELIMITER, SEMICOLON, COMMA, SPACE
