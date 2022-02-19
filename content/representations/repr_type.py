try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import ClassType


class ReprType(ClassType):
    BooleanRepr = 'BooleanRepr'
    NumericRepr = 'NumericRepr'
    StringRepr = 'StringRepr'
    SequenceRepr = 'SequenceRepr'


ReprType.prepare()
