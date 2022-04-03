from typing import Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto
    from utils.algo import JoinType
    from content.fields.field_interface import FieldInterface, ValueType
    from content.fields.field_role_type import FieldRoleType
    from content.format.format_interface import ContentFormatInterface, Compress
    from content.items.item_classes import *
    from content.struct.struct_interface import StructInterface, StructMixinInterface
    from content.struct.struct_row_interface import StructRowInterface
    from content.terms.term_type import TermType, TermDataAttribute, TermRelation
    from content.terms.term_interface import TermInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.classes.auto import Auto
    from ..utils.algo import JoinType
    from .fields.field_interface import FieldInterface, ValueType
    from .fields.field_role_type import FieldRoleType
    from .format.format_interface import ContentFormatInterface, Compress
    from .items.item_classes import *
    from .struct.struct_interface import StructInterface, StructMixinInterface
    from .struct.struct_row_interface import StructRowInterface
    from .terms.term_type import TermType, TermDataAttribute, TermRelation
    from .terms.term_interface import TermInterface

Field = Union[FieldID, FieldInterface]
Struct = StructInterface
Group = Union[Struct, Array]
FieldOrStruct = Union[FieldInterface, StructInterface]
FieldOrGroup = Union[Field, Group]
UniKey = Union[StructInterface, Array, FieldID, Auto, Callable]
StructRow = StructRowInterface
RegularItem = Union[SimpleItem, StructRow]
Item = Union[Any, RegularItem]
How = Union[JoinType, str]
