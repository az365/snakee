try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import ClassType


class FieldRoleType(ClassType):
    Undefined = 'undef'
    Id = 'id'
    Name = 'name'
    Repr = 'repr'
    Key = 'key'
    Ids = 'ids'
    Count = 'count'
    Share = 'share'


FieldRoleType.prepare()
FieldRoleType.add_classes(
    id=int, name=str, repr=str, key=str, ids=tuple,
    count=int, share=float,
)
