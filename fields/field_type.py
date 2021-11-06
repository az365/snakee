from inspect import isclass
import json

try:  # Assume we're a sub-module in a package.
    from utils.arguments import any_to_bool, safe_converter, get_value
    from utils.enum import DynamicEnum
    from utils.decorators import deprecated_with_alternative
    from connectors.databases.dialect_type import DialectType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils.arguments import any_to_bool, safe_converter, get_value
    from ..utils.enum import DynamicEnum
    from ..utils.decorators import deprecated_with_alternative
    from ..connectors.databases.dialect_type import DialectType


class FieldType(DynamicEnum):
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

    _dict_heuristic_suffix_to_type = dict()
    _dict_dialect_types = dict()

    @classmethod
    def get_heuristic_suffix_to_type(cls) -> dict:
        return cls._dict_heuristic_suffix_to_type

    @classmethod
    def get_dialect_types(cls) -> dict:
        return cls._dict_dialect_types

    def get_type_in(self, dialect):
        type_props = self.get_dialect_types()[self.get_value()]
        dialect_value = get_value(dialect)
        return type_props[dialect_value]

    def get_py_type(self):
        return self.get_type_in(dialect='py')

    def isinstance(self, value) -> bool:
        py_type = self.get_py_type()
        if py_type == float:
            return isinstance(value, (int, float))
        else:
            return isinstance(value, py_type)

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

    @classmethod
    def get_canonic_type(cls, field_type, ignore_absent: bool = False):
        if isinstance(field_type, FieldType):
            return field_type
        elif field_type in FieldType.__dict__.values():
            return FieldType(field_type)
        elif field_type == 'integer':
            field_type = FieldType.Int
            assert isinstance(field_type, FieldType)
            return field_type
        else:
            field_types_by_dialect = cls.get_dialect_types()
            for canonic_type, dict_names in sorted(field_types_by_dialect.items(), key=lambda i: i[0], reverse=True):
                for dialect, type_name in dict_names.items():
                    if field_type == type_name:
                        return FieldType(canonic_type)
        if not ignore_absent:
            raise ValueError('Unsupported field type: {}'.format(field_type))

    def get_converter(self, source, target):
        source_dialect_name = get_value(source)
        target_dialect_name = get_value(target)
        converter_name = '{}_to_{}'.format(source_dialect_name, target_dialect_name)
        field_types_by_dialect = self.get_dialect_types()
        types_by_dialects = field_types_by_dialect.get(self, {})
        return types_by_dialects.get(converter_name, str)

    def is_str(self) -> bool:
        return self.get_value().startswith('str')

    def check_value(self, value) -> bool:
        py_type = self.get_py_type()
        return isinstance(value, py_type)


FieldType.prepare()
FieldType._dict_dialect_types = {
    FieldType.Any: dict(py=str, pg='text', ch='String', str_to_py=str),
    FieldType.Json: dict(py=dict, pg='text', ch='String', str_to_py=json.loads, py_to_str=json.dumps),
    FieldType.Str: dict(py=str, pg='text', ch='String', str_to_py=str),
    FieldType.Str16: dict(py=str, pg='varchar(16)', ch='FixedString(16)', str_to_py=str),
    FieldType.Str64: dict(py=str, pg='varchar(64)', ch='FixedString(64)', str_to_py=str),
    FieldType.Str256: dict(py=str, pg='varchar(256)', ch='FixedString(256)', str_to_py=str),
    FieldType.Int: dict(py=int, pg='int', ch='Int32', str_to_py=safe_converter(int)),
    FieldType.Float: dict(py=float, pg='numeric', ch='Float32', str_to_py=safe_converter(float)),
    FieldType.IsoDate: dict(py=str, pg='date', ch='Date', str_to_py=str),
    FieldType.Bool: dict(py=bool, pg='bool', ch='UInt8', str_to_py=any_to_bool, py_to_ch=safe_converter(int)),
    FieldType.Tuple: dict(py=tuple, pg='text', str_to_py=safe_converter(eval, tuple())),
    FieldType.Dict: dict(py=dict, pg='text', str_to_py=safe_converter(eval, dict())),
}
FieldType._dict_heuristic_suffix_to_type = {
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


@deprecated_with_alternative('FieldType.get_canonic_type()')
def get_canonic_type(field_type, ignore_absent: bool = False) -> FieldType:
    field_type = FieldType.get_canonic_type(field_type, ignore_absent=ignore_absent)
    assert isinstance(field_type, FieldType)
    return field_type


@deprecated_with_alternative('FieldType.detect_by_name()')
def detect_field_type_by_name(field_name) -> FieldType:
    field_type = FieldType.detect_by_name(field_name)
    assert isinstance(field_type, FieldType)
    return field_type
