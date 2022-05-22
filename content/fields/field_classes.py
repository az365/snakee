from typing import Optional, Union, Any

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructInterface, FieldInterface, RepresentationInterface, SelectionLoggerInterface,
        Auto, AUTO,
    )
    from base.functions.arguments import update
    from utils.decorators import deprecated_with_alternative
    from content.value_type import ValueType
    from content.fields.field_role_type import FieldRoleType
    from content.struct.flat_struct import FlatStruct
    from content.fields.any_field import AnyField, FieldEdgeType
    from content.fields.id_field import IdField
    from content.fields.name_field import NameField
    from content.fields.repr_field import ReprField
    from content.fields.key_field import KeyField
    from content.fields.ids_field import IdsField
    from content.fields.count_field import CountField
    from content.fields.share_field import ShareField
    from content.fields.value_field import ValueField
    from content.fields.mean_field import MeanField
    from content.fields.norm_field import NormField
    from content.fields.rate_field import RateField
    from content.fields.cat_field import CatField
    from content.fields.series_field import SeriesField
    from content.selection.abstract_expression import AbstractDescription
    from content.selection import concrete_expression as ce
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructInterface, FieldInterface, RepresentationInterface, SelectionLoggerInterface,
        Auto, AUTO,
    )
    from ...base.functions.arguments import update
    from ...utils.decorators import deprecated_with_alternative
    from ..value_type import ValueType
    from .field_role_type import FieldRoleType
    from ..struct.flat_struct import FlatStruct
    from .any_field import AnyField, FieldEdgeType
    from .id_field import IdField
    from .name_field import NameField
    from .repr_field import ReprField
    from .key_field import KeyField
    from .ids_field import IdsField
    from .count_field import CountField
    from .share_field import ShareField
    from .value_field import ValueField
    from .mean_field import MeanField
    from .norm_field import NormField
    from .rate_field import RateField
    from .cat_field import CatField
    from .series_field import SeriesField
    from ..selection.abstract_expression import AbstractDescription
    from ..selection import concrete_expression as ce

Type = Union[ValueType, FieldRoleType, type, Auto]

_logger = None

FieldRoleType.add_classes(
    id=IdField, name=NameField, repr=ReprField, key=KeyField, ids=IdsField,
    count=CountField, share=ShareField, rate=RateField,
    value=ValueField, mean=MeanField, norm=NormField,
    cat=CatField, series=SeriesField,
)


def get_logger() -> Optional[SelectionLoggerInterface]:
    global _logger
    return _logger


def set_logger(logger: SelectionLoggerInterface):
    global _logger
    _logger = logger


def field(
        name: str,
        field_type: Type = AUTO,
        role: FieldRoleType = FieldRoleType.Undefined,
        representation: RepresentationInterface = None,
        default: Optional[Any] = None,
        caption: Optional[str] = None,
        **kwargs
) -> AnyField:
    if field_type:
        assert 'value_type' not in kwargs
        kwargs['value_type'] = field_type
    if default:
        assert 'default_value' not in kwargs
        kwargs['default_value'] = default
    if role in (FieldRoleType.Undefined, AUTO, None):
        field_class = AnyField
    else:
        field_class = role.get_class()
    return field_class(
        name,
        representation=representation,
        caption=caption,
        logger=_logger,
        **kwargs,
    )


def struct(
        *fields, default_type: Type = AUTO,
        name: Optional[str] = None, caption: Optional[str] = None,
        **kwargs
) -> FlatStruct:
    fields = update(fields)
    return FlatStruct(fields, name=name, caption=caption, default_type=default_type, **kwargs)


@deprecated_with_alternative('struct')
def group(*fields, **kwargs) -> FlatStruct:
    return FlatStruct(fields, **kwargs)


def const(value: Any):  # -> RegularDescription
    return ce.RegularDescription(target='_', function = lambda i: value, inputs=[], target_item_type=AUTO)
