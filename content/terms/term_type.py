try:  # Assume we're a submodule in a package.
    from base.classes.enum import DynamicEnum, ClassType
    from content.fields.field_role_type import FieldRoleType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import DynamicEnum, ClassType
    from ..fields.field_role_type import FieldRoleType


class TermType(ClassType):
    Continual = 'continual'
    Process = 'process'
    Discrete = 'discrete'
    Object = 'object'
    Hierarchy = 'hierarchy'


class TermDataAttribute(ClassType):
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
    Process = 'process'
    Object = 'object'

    _pairs = [
        (OneToMany, ManyToOne),
        (Parent, Child),
        (Process, Object),
    ]

    def get_reversed(self):
        for pair in self._pairs:
            if self in pair:
                reversed_relation = pair[0] if self == pair[1] else pair[1]
                return TermRelation(reversed_relation)
        return self


TermType.prepare()
TermDataAttribute.prepare()
TermRelation.prepare()

FieldRole = FieldRoleType  # deprecated
