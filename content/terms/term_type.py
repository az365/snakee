try:  # Assume we're a submodule in a package.
    from base.classes.enum import DynamicEnum, ClassType
    from content.fields.field_role_type import FieldRoleType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import DynamicEnum, ClassType
    from ..fields.field_role_type import FieldRoleType


class TermType(ClassType):
    Continual = 'continual'
    Discrete = 'discrete'
    Object = 'object'
    Hierarchy = 'hierarchy'


class TermDataAttribute(DynamicEnum):
    Fields = 'fields'
    Dictionaries = 'dictionaries'
    Mappers = 'mappers'
    Datasets = 'datasets'
    Relations = 'relations'


class TermRelation(DynamicEnum):
    OneToOne = 'one_to_one'
    OneToMany = 'one_to_many'
    ManyToOne = 'many_to_one'
    Parent = 'parent'
    Child = 'child'

    _pairs = [
        (OneToMany, ManyToOne),
        (Parent, Child),
    ]

    def get_reverted(self):
        for pair in self._pairs:
            if self in pair:
                return pair[0] if self == pair[1] else pair[1]
        return self


TermType.prepare()
TermDataAttribute.prepare()
TermRelation.prepare()

FieldRole = FieldRoleType  # deprecated
