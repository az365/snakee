try:  # Assume we're a submodule in a package.
    from content.representations.repr_type import ReprType
    from content.representations.repr_interface import RepresentationInterface
    from content.representations.abstract_repr import AbstractRepresentation
    from content.representations.boolean_repr import BooleanRepresentation
    from content.representations.numeric_repr import NumericRepresentation
    from content.representations.string_repr import StringRepresentation
    from content.representations.sequence_repr import SequenceRepresentation
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .repr_type import ReprType
    from .repr_interface import RepresentationInterface
    from .abstract_repr import AbstractRepresentation
    from .boolean_repr import BooleanRepresentation
    from .numeric_repr import NumericRepresentation
    from .string_repr import StringRepresentation
    from .sequence_repr import SequenceRepresentation

ReprType.add_classes(
    BooleanRepr=BooleanRepresentation,
    NumericRepr=NumericRepresentation,
    StringRepr=StringRepresentation,
    SequenceRepr=SequenceRepresentation,
)
