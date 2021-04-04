from enum import Enum
from inspect import isclass
import json


class FieldType(Enum):
    Any = 'any'
    Json = 'json'
    Str = 'str'
    Str16 = 'str16'
    Str64 = 'str64'
    Str256 = 'str256'
    Int = 'int'
    Float = 'float'
    IsoDate = 'date'
    Bool = 'bool'
    Tuple = 'tuple'
    Dict = 'dict'

    def get_name(self):
        return self.value

    @staticmethod
    def get_heuristic_suffix_to_type():
        return {
            'hist': FieldType.Dict,
            'names': FieldType.Tuple,
            'ids': FieldType.Tuple,
            'id': FieldType.Int,
            'hits': FieldType.Int,
            'count': FieldType.Int,
            'sum': FieldType.Float,
            'avg': FieldType.Float,
            'mean': FieldType.Float,
            'share': FieldType.Float,
            'norm': FieldType.Float,
            'weight': FieldType.Float,
            'value': FieldType.Float,
            'score': FieldType.Float,
            'rate': FieldType.Float,
            'coef': FieldType.Float,
            'abs': FieldType.Float,
            'rel': FieldType.Float,
            'is': FieldType.Bool,
            'has': FieldType.Bool,
            None: FieldType.Str,
        }

    @classmethod
    def detect_by_name(cls, field_name: str):
        name_parts = field_name.split('_')
        heuristic_suffix_to_type = cls.get_heuristic_suffix_to_type()
        default_type = heuristic_suffix_to_type[None]
        field_type = default_type
        for suffix in heuristic_suffix_to_type:
            if suffix in name_parts:
                field_type = heuristic_suffix_to_type[suffix]
                break
        return field_type

    @staticmethod
    def detect_by_type(field_type):
        if isclass(field_type):
            field_type = field_type.__name__
        return FieldType(str(field_type))

    @staticmethod
    def detect_by_value(value):
        field_type = type(value)
        field_type_name = str(field_type)
        return FieldType(field_type_name)


def any_to_bool(value):
    if isinstance(value, str):
        return value not in ('False', 'false', 'None', 'none', 'no', '0', '')
    else:
        return bool(value)


def safe_converter(converter, default_value=0):
    def func(value):
        if value is None or value == '':
            return default_value
        else:
            try:
                return converter(value)
            except ValueError:
                return default_value
    return func


DIALECTS = ('str', 'py', 'pg', 'ch')
FIELD_TYPES = {
    FieldType.Any.value: dict(py=str, pg='text', ch='String', str_to_py=str),
    FieldType.Json.value: dict(py=dict, pg='text', ch='String', str_to_py=json.loads, py_to_str=json.dumps),
    FieldType.Str.value: dict(py=str, pg='text', ch='String', str_to_py=str),
    FieldType.Str16.value: dict(py=str, pg='varchar(16)', ch='FixedString(16)', str_to_py=str),
    FieldType.Str64.value: dict(py=str, pg='varchar(64)', ch='FixedString(64)', str_to_py=str),
    FieldType.Str256.value: dict(py=str, pg='varchar(256)', ch='FixedString(256)', str_to_py=str),
    FieldType.Int.value: dict(py=int, pg='int', ch='Int32', str_to_py=safe_converter(int)),
    FieldType.Float.value: dict(py=float, pg='numeric', ch='Float32', str_to_py=safe_converter(float)),
    FieldType.IsoDate.value: dict(py=str, pg='date', ch='Date', str_to_py=str),
    FieldType.Bool.value: dict(py=bool, pg='bool', ch='UInt8', str_to_py=any_to_bool, py_to_ch=safe_converter(int)),
    FieldType.Tuple.value: dict(py=tuple, pg='text', str_to_py=safe_converter(eval, tuple())),
    FieldType.Dict.value: dict(py=dict, pg='text', str_to_py=safe_converter(eval, dict())),
}
AGGR_HINTS = (None, 'id', 'cat', 'measure')
HEURISTIC_SUFFIX_TO_TYPE = FieldType.get_heuristic_suffix_to_type()


def get_canonic_type(field_type, ignore_absent=False):
    if isinstance(field_type, FieldType):
        return field_type
    elif field_type in FieldType.__dict__.values():
        return FieldType(field_type)
    else:
        for canonic_type, dict_names in sorted(FIELD_TYPES.items(), key=lambda i: i[0], reverse=True):
            for dialect, type_name in dict_names.items():
                if field_type == type_name:
                    return FieldType(canonic_type)
    if not ignore_absent:
        raise ValueError('Unsupported field type: {}'.format(field_type))


def detect_field_type_by_name(field_name):
    name_parts = field_name.split('_')
    default_type = HEURISTIC_SUFFIX_TO_TYPE[None]
    field_type = default_type
    for suffix in HEURISTIC_SUFFIX_TO_TYPE:
        if suffix in name_parts:
            field_type = HEURISTIC_SUFFIX_TO_TYPE[suffix]
            break
    return field_type
