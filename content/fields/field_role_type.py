try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
    from content.value_type import ValueType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import ClassType
    from ..value_type import ValueType


class FieldRoleType(ClassType):
    Undefined = 'undef'
    Id = 'id'
    Name = 'name'
    Repr = 'repr'
    Key = 'key'
    Ids = 'ids'
    Count = 'count'
    Share = 'share'
    Rate = 'rate'
    Value = 'value'
    Mean = 'mean'
    Norm = 'norm'
    Cat = 'cat'
    Series = 'series'

    _dict_value_types = dict(
        undef=ValueType.Any,
        id=ValueType.Int,
        name=ValueType.Str,
        repr=ValueType.Str,
        key=ValueType.Str,
        ids=ValueType.Sequence,
        count=ValueType.Int,
        share=ValueType.Float,
        rate=ValueType.Float,
        value=ValueType.Float,
        mean=ValueType.Float,
        norm=ValueType.Float,
        cat=ValueType.Str,
        series=ValueType.Sequence,
    )

    @classmethod
    def get_dict_value_types(cls) -> dict:
        return cls._dict_value_types

    def get_default_value_type(self, default: ValueType = ValueType.Any) -> ValueType:
        dict_types = self.get_dict_value_types()
        assert dict_types, 'value-types must be defined by set_dict_classes() method'
        found_type = dict_types.get(self)
        if found_type:
            return found_type
        else:
            return default


FieldRoleType.prepare()
FieldRoleType.set_default(FieldRoleType.Undefined)
