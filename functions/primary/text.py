from typing import Optional
import sys
import csv
import re

RE_LETTERS = re.compile('[^a-zа-я ]')

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
