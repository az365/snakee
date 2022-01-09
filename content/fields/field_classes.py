from typing import Optional, Union, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from interfaces import StructInterface, FieldInterface, SelectionLoggerInterface, Auto, AUTO
    from content.fields.field_type import FieldType
    from content.struct.flat_struct import FlatStruct
    from content.fields.abstract_field import AbstractField
    from content.fields.advanced_field import AdvancedField
    from content.selection.abstract_expression import AbstractDescription
    from content.selection import concrete_expression as ce
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import StructInterface, FieldInterface, SelectionLoggerInterface, Auto, AUTO
    from .field_type import FieldType
    from ..struct.flat_struct import FlatStruct
    from .abstract_field import AbstractField
    from .advanced_field import AdvancedField
    from ..selection.abstract_expression import AbstractDescription
    from ..selection import concrete_expression as ce

Type = Union[FieldType, type, Auto]

_logger = None


def get_logger() -> Optional[SelectionLoggerInterface]:
    global _logger
    return _logger


def set_logger(logger: SelectionLoggerInterface):
    global _logger
    _logger = logger


def field(
        name: str, field_type: Type = AUTO, default: Optional[Any] = None,
        caption: Optional[str] = None,
) -> AdvancedField:
    return AdvancedField(name, field_type=field_type, caption=caption, default=default, logger=_logger)


def struct(
        *fields, default_type: Type = AUTO,
        name: Optional[str] = None, caption: Optional[str] = None,
        **kwargs
) -> FlatStruct:
    fields = arg.update(fields)
    return FlatStruct(fields, name=name, caption=caption, default_type=default_type, **kwargs)


def group(*fields, **kwargs) -> FlatStruct:
    return FlatStruct(fields, **kwargs)
