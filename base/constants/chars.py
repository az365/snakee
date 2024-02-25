EMPTY = ''
SPACE = ' '
DOT = '.'
COMMA = ','
COLON = ':'
SEMICOLON = ';'
STAR = '*'
CROSS = 'x'
PIPE = '|'
PLUS = '+'
MINUS = '-'
UNDER = '_'
EQUAL = '='
TILDA = '~'
SHARP = HASH = '#'
SLASH = '/'
BACKSLASH = '\\'

ALL = STAR
COVERT = STAR * 3  # '***'
DEFAULT_STR = MINUS
ITEM = MINUS
BULLET = STAR
DEL = CROSS
ABOUT = TILDA
FILL_CHAR = SPACE
REPR_DELIMITER = SPACE
NOT_SET = UNDER
PROTECTED = UNDER
SHORT_CROP_SUFFIX = UNDER
CROP_SUFFIX = DOT * 2  # '..'
ELLIPSIS = DOT * 3  # '...'
EQUALITY = EQUAL * 2  # '=='
TITLE_PREFIX = EQUAL * 3  # '==='
DOUBLE_SPACE = SPACE * 2  # '  '
SMALL_INDENT = DOUBLE_SPACE  # '  '
TAB_INDENT = SPACE * 4  # '    '
REQUEST_DELIMITER = SEMICOLON + SPACE  # '; '
ITEMS_DELIMITER = COMMA + SPACE  # ', '
DEFAULT_ITEMS_DELIMITER = ITEMS_DELIMITER  # ', '
IDS_DELIMITER = PIPE  # '|'

TAB_CHAR, TAB_SUBSTITUTE = '\t', ' -> '
RETURN_CHAR, RETURN_SUBSTITUTE = '\r', ' <- '
PARAGRAPH_CHAR, PARAGRAPH_SUBSTITUTE = '\n', ' \\n '

KV_DELIMITER = EQUAL  # '='
ARG_DELIMITER = COMMA + SPACE  # ', '
ANN_DELIMITER = COLON + SPACE  # ': '
OS_EXT_DELIMITER = DOT  # '.'
OS_PARENT_PATH = DOT * 2  # '..'
OS_PLACEHOLDER = ALL  # '*'
PY_PLACEHOLDER = '{}'
PY_COMMENT = MD_HEADER = HASH
PY_INDENT = TAB_INDENT
SQL_INDENT = TAB_INDENT
HTML_INDENT = TAB_INDENT
HTML_SPACE = '&nbsp;'

TYPE_CHARS = dict(
    bool='&',
    int='#',
    float='%',
    str='$',
    list='*',
    tuple='*',
    set='*',
    DataFrame='#',
    obj='@',
    none='-',
)
TYPE_EMOJI = dict(
    bool='📌',
    int='#️⃣',
    float='#️⃣',
    str='📝',
    list='📂',
    tuple='📁',
    set='💠',
    DataFrame='📊',
    obj='🌳',
    none='♦',
)
