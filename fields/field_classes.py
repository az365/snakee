from typing import Optional, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.databases import dialect as di
    from fields.field_type import FieldType
    from fields.schema_interface import SchemaInterface
    from fields.field_interface import FieldInterface
    from fields.field_group import FieldGroup
    from fields.abstract_field import AbstractField
    from fields.advanced_field import AdvancedField
    from selection.abstract_expression import AbstractDescription
    from selection import concrete_expression as ce
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..connectors.databases import dialect as di
    from .field_type import FieldType
    from .schema_interface import SchemaInterface
    from .field_interface import FieldInterface
    from .field_group import FieldGroup
    from .abstract_field import AbstractField
    from .advanced_field import AdvancedField
    from ..selection.abstract_expression import AbstractDescription
    from ..selection import concrete_expression as ce
    from ..loggers.selection_logger_interface import SelectionLoggerInterface

Type = Union[FieldType, type, arg.DefaultArgument]

_logger = None


def get_logger() -> Optional[SelectionLoggerInterface]:
    global _logger
    return _logger


def set_logger(logger: SelectionLoggerInterface):
    global _logger
    _logger = logger


def field(
        name: str, field_type: Type = arg.DEFAULT, default: Optional[Any] = None,
        caption: Optional[str] = None,
) -> AdvancedField:
    return AdvancedField(name, field_type=field_type, caption=caption, default=default, logger=_logger)


def group(*fields) -> FieldGroup:
    fields = arg.update(fields)
    return FieldGroup(fields)
