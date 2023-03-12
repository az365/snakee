from typing import Type, Optional, Callable, Tuple, Union, Any

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
Options = Optional[dict]
Links = Optional[dict]
