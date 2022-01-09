from typing import Optional, Callable, Iterable, Union, Type
from inspect import isclass
import sys
import csv
import re

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


def get_str_from_args_kwargs(
        *args,
        delimiter: str = '=',
        remove_prefixes: Optional[Iterable] = None,
        **kwargs
) -> str:
    list_str_from_kwargs = list()
    for k, v in kwargs.items():
        if isclass(v):
            v_str = v.__name__
        else:
            v_str = v.__repr__()
        for prefix in remove_prefixes or []:
            if v_str.startswith(prefix):
                v_str = v_str[len(prefix):]
        list_str_from_kwargs.append('{}{}{}'.format(k, delimiter, v_str))
    list_str_from_args = [str(i) for i in args]
    return ', '.join(list_str_from_args + list_str_from_kwargs)


def get_str_from_annotation(class_or_func: Union[Callable, Type]) -> str:
    if isclass(class_or_func):
        func = class_or_func.__init__
        name = class_or_func.__name__
    elif isinstance(class_or_func, Callable):
        func = class_or_func
        name = class_or_func.__name__
    elif isinstance(class_or_func, object):
        func = class_or_func.__class__.__init__
        name = class_or_func.__class__.__name__
    else:
        raise TypeError
    if hasattr(func, '__annotations__'):
        ann_dict = func.__annotations__
        ann_str = get_str_from_args_kwargs(**ann_dict, delimiter=': ', remove_prefixes=['typing.'])
    else:
        ann_str = '*args, **kwargs'
    return '{}({})'.format(name, ann_str)
