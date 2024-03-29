from typing import Optional
import sys
import csv

try:
    from base.constants.chars import EMPTY, SPACE, COLON, SLASH, BACKSLASH, TAB_CHAR, PARAGRAPH_CHAR
    from base.constants.chars import OS_PLACEHOLDER, PY_PLACEHOLDER, DOUBLE_SPACE
    from base.constants.text import RE_LETTERS, NORM_LETTER_PAIRS, FALSE_VALUES
    from base.functions.arguments import get_str_from_args_kwargs, get_str_from_annotation
except ImportError:
    from ...base.constants.chars import EMPTY, SPACE, COLON, SLASH, BACKSLASH, TAB_CHAR, PARAGRAPH_CHAR
    from ...base.constants.chars import OS_PLACEHOLDER, PY_PLACEHOLDER, DOUBLE_SPACE
    from ...base.constants.text import RE_LETTERS, NORM_LETTER_PAIRS, FALSE_VALUES
    from ...base.functions.arguments import get_str_from_args_kwargs, get_str_from_annotation

max_int = sys.maxsize
while True:  # To prevent _csv.Error: field larger than field limit (131072)
    try:  # decrease the max_int value by factor 10 as long as the OverflowError occurs.
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)


def split_csv_row(line: str, delimiter: Optional[str] = None) -> list:
    if delimiter is None:
        rows = csv.reader([line])
    else:
        rows = csv.reader([line], delimiter)
    for r in rows:
        return r


def remove_extra_spaces(text: str) -> str:
    if PARAGRAPH_CHAR in text:
        text = text.replace(PARAGRAPH_CHAR, SPACE)
    while DOUBLE_SPACE in text:
        text = text.replace(DOUBLE_SPACE, SPACE)
    if text.startswith(SPACE):
        text = text[1:]
    if text.endswith(SPACE):
        text = text[:-1]
    return text


def norm_text(text: str) -> str:
    if text is not None:
        text = str(text).lower().replace(TAB_CHAR, SPACE)
        for a, c in NORM_LETTER_PAIRS:
            if a in text:
                text = text.replace(a, c)
        text = RE_LETTERS.sub(EMPTY, text)
        text = remove_extra_spaces(text)
        return text


def is_absolute_path(path: str) -> bool:
    return path.startswith(SLASH) or path.startswith(BACKSLASH) or COLON in path


def is_mask(string: str, count: Optional[int] = None, placeholder: str = OS_PLACEHOLDER) -> bool:
    if isinstance(string, str):
        if count:
            return len(string.split(placeholder)) == count + 1
        else:
            return len(string.split(placeholder)) > 1


def is_formatter(string: str, count: Optional[int] = None) -> bool:
    if isinstance(string, str):
        if count:
            return min([is_mask(string, count, placeholder=s) for s in PY_PLACEHOLDER])
        else:
            return min([is_mask(string, placeholder=s) for s in PY_PLACEHOLDER])
