try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
    from content.fields.field_type import FieldType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import ClassType
    from .field_type import FieldType


class FieldRoleType(ClassType):
    Undefined = 'undef'
    Id = 'id'
    Name = 'name'
    Repr = 'repr'
    Key = 'key'
    Ids = 'ids'
    Count = 'count'
    Share = 'share'
    Value = 'value'
    Rate = 'rate'

    _dict_value_types = dict(
        undef=FieldType.Any,
        id=FieldType.Int,
        name=FieldType.Str,
        repr=FieldType.Str,
        key=FieldType.Str,
        ids=FieldType.Tuple,
        count=FieldType.Int,
        share=FieldType.Float,
        value=FieldType.Float,
        rate=FieldType.Float,
    )

    @classmethod
    def get_dict_value_types(cls) -> dict:
        return cls._dict_value_types

    def get_default_value_type(self, default: FieldType = FieldType.Any) -> FieldType:
        dict_types = self.get_dict_value_types()
        assert dict_types, 'value-types must be defined by set_dict_classes() method'
        found_type = dict_types.get(self)
        if found_type:
            return found_type
        else:
            return default


FieldRoleType.prepare()
FieldRoleType.set_default(FieldRoleType.Undefined)
FieldRoleType.add_classes(
    id=int, name=str, repr=str, key=str, ids=tuple,
    count=int, share=float, value=float, rate=float,
)
