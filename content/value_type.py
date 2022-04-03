from inspect import isclass
import json

try:  # Assume we're a submodule in a package.
    from utils.arguments import any_to_bool, safe_converter, get_value
    from utils.decorators import deprecated_with_alternative
    from base.classes.enum import DynamicEnum
    from connectors.databases.dialect_type import DialectType
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from utils.arguments import any_to_bool, safe_converter, get_value
    from utils.decorators import deprecated_with_alternative
    from base.classes.enum import DynamicEnum
    from connectors.databases.dialect_type import DialectType
    from functions.primary import numeric as nm


class ValueType(DynamicEnum):
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
    Sequence = 'sequence'
    Dict = 'dict'

    _dict_heuristic_suffix_to_type = dict()
    _dict_dialect_types = dict()

    @classmethod
    def get_heuristic_suffix_to_type(cls) -> dict:
        return cls._dict_heuristic_suffix_to_type

    @classmethod
    def get_dialect_types(cls) -> dict:
        return cls._dict_dialect_types

    def get_type_in(self, dialect: DialectType):
        type_props = self.get_dialect_types()[self.get_value()]
        dialect_value = get_value(dialect)
        type_in_dialect = type_props.get(dialect_value)
        if not type_in_dialect:
            type_in_dialect = type_props[DialectType.get_default()]
        return type_in_dialect

    def get_py_type(self):
        return self.get_type_in(dialect=DialectType.Python)

    def isinstance(self, value) -> bool:
        if self == ValueType.Any:
            return True
        py_type = self.get_py_type()
        if py_type == float:
            return nm.is_numeric(value)
        elif py_type in (list, tuple):
            return isinstance(value, (list, tuple))
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
        return ValueType(str(field_type))

    @staticmethod
    def detect_by_value(value):
        field_type = type(value)
        field_type_name = str(field_type)
        return ValueType(field_type_name)

    @classmethod
    def get_canonic_type(cls, field_type, ignore_missing: bool = False, default='any'):
        if ignore_missing and field_type is None:
            field_type = default
        if isinstance(field_type, ValueType):
            return field_type
        elif field_type in ValueType.__dict__.values():
            return ValueType(field_type)
        elif field_type == 'integer':
            field_type = ValueType.Int
            assert isinstance(field_type, ValueType)
            return field_type
        else:
            field_types_by_dialect = cls.get_dialect_types()
            for canonic_type, dict_names in sorted(field_types_by_dialect.items(), key=lambda i: i[0], reverse=True):
                for dialect, type_name in dict_names.items():
                    if field_type == type_name:
                        return ValueType(canonic_type)
        str_field_type = str(field_type).split('.')[-1].lower()
        try:
            return ValueType(str_field_type)
        except ValueError as e:
            if ignore_missing:
                return ValueType(default)
            else:
                raise ValueError('Unsupported field type: {} ({})'.format(field_type, e))

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


ValueType.prepare()

ValueType._dict_dialect_types = {
    ValueType.Any: dict(py=str, pg='text', ch='String', str_to_py=str),
    ValueType.Json: dict(py=dict, pg='text', ch='String', str_to_py=json.loads, py_to_str=json.dumps),
    ValueType.Str: dict(py=str, pg='text', ch='String', str_to_py=str),
    ValueType.Str16: dict(py=str, pg='varchar(16)', ch='FixedString(16)', str_to_py=str),
    ValueType.Str64: dict(py=str, pg='varchar(64)', ch='FixedString(64)', str_to_py=str),
    ValueType.Str256: dict(py=str, pg='varchar(256)', ch='FixedString(256)', str_to_py=str),
    ValueType.Int: dict(py=int, pg='int', ch='Int32', str_to_py=safe_converter(int)),
    ValueType.Float: dict(py=float, pg='numeric', ch='Float32', str_to_py=safe_converter(float)),
    ValueType.IsoDate: dict(py=str, pg='date', ch='Date', str_to_py=str),
    ValueType.Bool: dict(py=bool, pg='bool', ch='UInt8', str_to_py=any_to_bool, py_to_ch=safe_converter(int)),
    ValueType.Sequence: dict(py=tuple, pg='text', str_to_py=safe_converter(eval, tuple())),
    ValueType.Dict: dict(py=dict, pg='text', str_to_py=safe_converter(eval, dict())),
}
ValueType._dict_heuristic_suffix_to_type = {
    'hist': ValueType.Dict,
    'names': ValueType.Sequence,
    'ids': ValueType.Sequence,
    'id': ValueType.Int,
    'hits': ValueType.Int,
    'count': ValueType.Int,
    'sum': ValueType.Float,
    'avg': ValueType.Float,
    'mean': ValueType.Float,
    'share': ValueType.Float,
    'norm': ValueType.Float,
    'weight': ValueType.Float,
    'value': ValueType.Float,
    'score': ValueType.Float,
    'rate': ValueType.Float,
    'coef': ValueType.Float,
    'abs': ValueType.Float,
    'rel': ValueType.Float,
    'pos': ValueType.Int,
    'no': ValueType.Int,
    'is': ValueType.Bool,
    'has': ValueType.Bool,
    None: ValueType.Str,
}
