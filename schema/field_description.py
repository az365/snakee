try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from fields import field_type as ft
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..fields import field_type as ft


class FieldDescription:
    def __init__(
            self,
            name,
            field_type=arg.DEFAULT,
            nullable=False,
            aggr_hint=None,
    ):
        self.name = name
        field_type = arg.undefault(field_type, ft.FieldType.Any)
        if field_type is None:
            self.field_type = ft.detect_field_type_by_name(name)
        else:
            self.field_type = ft.get_canonic_type(field_type)
        assert isinstance(nullable, bool)
        self.nullable = nullable
        assert aggr_hint in ft.AGGR_HINTS
        self.aggr_hint = aggr_hint

    def get_field_type(self):
        return self.get_type_in(None)

    def get_type_in(self, dialect):
        if dialect is None:
            return self.field_type.value
        else:
            assert dialect in ft.DIALECTS
            return ft.FIELD_TYPES.get(self.field_type.value, {}).get(dialect)

    def get_converter(self, source, target):
        converter_name = '{}_to_{}'.format(source, target)
        return ft.FIELD_TYPES.get(self.field_type.value, {}).get(converter_name, str)

    def check_value(self, value):
        py_type = self.get_type_in('py')
        return isinstance(value, py_type)

    def get_tuple(self):
        return self.name, self.field_type, self.nullable, self.aggr_hint

    def __str__(self):
        return ', '.join(map(str, self.get_tuple()))
