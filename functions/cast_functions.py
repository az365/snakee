from typing import Callable, Union

FieldType = Union[type, str]

DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)


def cast(field_type: FieldType, default_int: int = 0) -> Callable:
    def func(value):
        cast_function = DICT_CAST_TYPES.get(field_type, field_type)
        if value in (None, 'None', '') and field_type in ('int', int, float):
            value = default_int
        return cast_function(value)
    return func


def percent(field_type: FieldType = float, round_digits: int = 1, default_value=None) -> Callable:
    def func(value) -> Union[int, float, str]:
        if value is None:
            return default_value
        else:
            cast_function = DICT_CAST_TYPES.get(field_type, field_type)
            value = round(100 * value, round_digits)
            value = cast_function(value)
            if cast_function == str:
                value += '%'
            return value
    return func
