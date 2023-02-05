from typing import Type, Optional, Callable, Tuple, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .auto import Auto, AUTO

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Value = Any
Class = Union[Type, Callable]
Array = Union[list, tuple]
ARRAY_TYPES = list, tuple
PRIMITIVE_TYPES = str, int, float, bool

Line = str
Record = dict
Row = tuple
FormattedRow = Tuple[str]

Name = FieldID
Count = Optional[int]
Message = Union[str, Array]
Columns = Optional[Array]
OptionalFields = Union[Array, str, None]
Options = Union[dict, Auto, None]
Links = Optional[dict]

AutoName = Union[Auto, Name, None]
AutoCount = Union[Auto, Count, None]
AutoBool = Union[Auto, bool, None]
AutoColumns = Union[Auto, Columns, None]
AutoLinks = Union[Auto, Links, None]
