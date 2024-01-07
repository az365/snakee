from typing import Type, Optional, Callable, Tuple, Union, Any

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Numeric = Union[int, float]
Value = Any
Class = Union[Type, Callable]
Array = Union[list, tuple]
Collection = Union[Array, set, dict]

NUMERIC_TYPES = int, float
PRIMITIVE_TYPES = str, bool, *NUMERIC_TYPES
ARRAY_TYPES = list, tuple
COLLECTION_TYPES = *ARRAY_TYPES, set, dict

Line = str
Record = dict
Row = tuple
FormattedRow = Tuple[str]

Name = FieldID
Count = Optional[int]
Message = Union[str, Array]
Columns = Optional[Array]
OptionalFields = Union[Array, str, None]
Options = Optional[dict]
Links = Optional[dict]
