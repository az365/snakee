try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from interfaces import StructInterface, FieldInterface, FieldType, DialectType
    from content.fields.simple_field import SimpleField
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import StructInterface, FieldInterface, FieldType, DialectType
    from .simple_field import SimpleField


class LegacyField(SimpleField, FieldInterface):
    @deprecated_with_alternative('fields.AdvancedField()')
    def __init__(
            self,
            name,
            field_type=arg.AUTO,
            nullable=False,
            aggr_hint=None,
    ):
        field_type = arg.acquire(field_type, FieldType.Any)
        if field_type is None:
            field_type = FieldType.detect_by_name(name)
        else:
            field_type = FieldType(field_type)
        super().__init__(name=name, field_type=field_type)
        assert isinstance(nullable, bool)
        self.nullable = nullable
        self.aggr_hint = aggr_hint

    def get_field_type(self):
        return self.get_type_in(None)

    def check_value(self, value):
        py_type = self.get_type_in(DialectType.Python)
        return isinstance(value, py_type)

    def get_tuple(self):
        return self.get_name(), self.get_type(), self.nullable, self.aggr_hint


FieldDescription = LegacyField
