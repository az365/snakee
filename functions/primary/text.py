from typing import Optional
import sys
import csv
import re

try:
    from base.functions.arguments import get_str_from_args_kwargs, get_str_from_annotation
except ImportError:
    from ...base.functions.arguments import get_str_from_args_kwargs, get_str_from_annotation

RE_LETTERS = re.compile('[^a-zа-я ]')
STR_FALSE_SYNONYMS = ('False', 'false', 'None', 'none', 'no', '0', '')

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
    if '\n' in text:
        text = text.replace('\n', ' ')
    while '  ' in text:
        text = text.replace('  ', ' ')
    if text.startswith(' '):
        text = text[1:]
    if text.endswith(' '):
        text = text[:-1]
    return text


def norm_text(text: str) -> str:
    if text is not None:
        text = str(text).lower().replace('\t', ' ')
        text = text.replace('ё', 'е')
        text = RE_LETTERS.sub('', text)
        text = remove_extra_spaces(text)
        return text


def is_absolute_path(path: str) -> bool:
    return path.startswith('/') or path.startswith('\\') or ':' in path


def is_mask(string: str, count=None, placeholder: str = '*') -> bool:
    if isinstance(string, str):
        if count:
            return len(string.split(placeholder)) == count + 1
        else:
            return len(string.split(placeholder)) > 1


def is_formatter(string: str, count=None) -> bool:
    if isinstance(string, str):
        if count:
            return min([is_mask(string, count, placeholder=s) for s in '{}'])
        else:
            return min([is_mask(string, count, placeholder=s) for s in '{}'])


def str_to_bool(line: str) -> bool:
    return line not in STR_FALSE_SYNONYMS
