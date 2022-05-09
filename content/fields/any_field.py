from typing import Type, Optional, Callable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from interfaces import (
        FieldInterface, RepresentationInterface, StructInterface, ExtLogger, SelectionLogger,
        ValueType, FieldRoleType, ReprType, ItemType, DialectType,
        ARRAY_TYPES, AUTO, Auto, AutoBool, AutoName, Class,
    )
    from base.functions.arguments import get_name, get_value, get_plural
    from base.abstract.simple_data import SimpleDataWrapper, EMPTY
    from base.mixin.map_data_mixin import MultiMapDataMixin
    from content.selection.selectable_mixin import SelectableMixin
    from content.selection import abstract_expression as ae, concrete_expression as ce
    from content.fields.field_edge_type import FieldEdgeType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        FieldInterface, RepresentationInterface, StructInterface, ExtLogger, SelectionLogger,
        ValueType, FieldRoleType, ReprType, ItemType, DialectType,
        ARRAY_TYPES, AUTO, Auto, AutoBool, AutoName, Class,
    )
    from ...base.functions.arguments import get_name, get_value, get_plural
    from ...base.abstract.simple_data import SimpleDataWrapper, EMPTY
    from ...base.mixin.map_data_mixin import MultiMapDataMixin
    from ..selection.selectable_mixin import SelectableMixin
    from ..selection import abstract_expression as ae, concrete_expression as ce
    from .field_edge_type import FieldEdgeType

Native = Union[SimpleDataWrapper, MultiMapDataMixin, FieldInterface]
AutoRepr = Union[RepresentationInterface, str, Auto]


class AnyField(SimpleDataWrapper, SelectableMixin, MultiMapDataMixin, FieldInterface):
    _struct_builder: Optional[Callable] = None

    def __init__(
            self,
            name: str,
            value_type: ValueType = ValueType.Any,
            representation: Union[RepresentationInterface, str, None] = None,
            caption: str = EMPTY,
            default_item_type: ItemType = ItemType.Any,
            default_value: Any = None,
            example_value: Any = None,
            is_valid: AutoBool = AUTO,
            skip_errors: bool = False,
            logger: Optional[SelectionLogger] = None,
            group_name: Optional[str] = None,  # deprecated
            group_caption: Optional[str] = None,  # deprecated
            data: Optional[dict] = None
    ):
        data = Auto.delayed_acquire(data, dict)
        value_type = Auto.delayed_acquire(value_type, ValueType.detect_by_name, field_name=name)
        value_type = ValueType.get_canonic_type(value_type, ignore_missing=True)
        assert isinstance(value_type, ValueType), 'Expected ValueType, got {}'.format(value_type)
        self._value_type = value_type
        self._representation = representation
        self._default_value = default_value
        self._example_value = example_value
        self._default_item_type = default_item_type
        self._skip_errors = skip_errors
        self._logger = logger
        self._is_valid = is_valid
        self._group_name = group_name or EMPTY  # deprecated
        self._group_caption = group_caption or EMPTY  # deprecated
        super().__init__(name=name, caption=caption, data=data)

    @staticmethod
    def get_role() -> FieldRoleType:
        return FieldRoleType.Undefined

    def set_role(self, role: FieldRoleType, inplace: bool = False) -> Native:
        if inplace:
            raise ValueError('FieldRoleType can be defined outplace only')
        else:
            return self.set_role_outplace(role)

    def set_role_outplace(self, role: FieldRoleType) -> Native:
        return role.build(**self.get_meta())

    def get_representation(self) -> Union[RepresentationInterface, str, None]:
        return self._representation

    def set_representation(self, representation: Union[RepresentationInterface, str], inplace: bool) -> Native:
        if inplace:
            self._representation = representation
            return self
        else:
            field = self.make_new(representation=representation)
            return self._assume_native(field)

    def set_repr(self, representation: AutoRepr = AUTO, inplace: bool = False, **kwargs) -> Native:
        assert inplace is not None and not Auto.is_auto(inplace)
        if Auto.is_auto(representation):
            representation = self.get_representation()
        if kwargs:
            if Auto.is_defined(representation):
                assert isinstance(representation, RepresentationInterface), 'got {}'.format(representation)
                representation = representation.update_meta(**kwargs, inplace=False)
            else:
                repr_class = self.get_repr_class()
                representation = repr_class(**kwargs)
        return self.set_representation(representation, inplace=inplace) or self

    def get_repr_class(self) -> Class:
        if self.is_numeric():
            return ReprType.NumericRepr.get_class()
        elif self.is_boolean():
            return ReprType.BooleanRepr.get_class()
        else:
            return ReprType.StringRepr.get_class()

    def get_type(self) -> ValueType:  # deprecated
        return self.get_value_type()  # self.get_field_role() = self.get_role_type() will be used

    def set_type(self, value_type: ValueType, inplace: bool) -> Native:  # deprecated
        return self.set_value_type(value_type=value_type, inplace=inplace)

    def get_value_type(self) -> ValueType:
        return self._value_type

    def set_value_type(self, value_type: ValueType, inplace: bool) -> Native:
        if inplace:
            self._value_type = value_type
            return self
        else:
            field = self.set_outplace(value_type=value_type)
            return self._assume_native(field)

    # @deprecated
    def get_type_name(self) -> str:
        return self.get_value_type_name()

    def get_value_type_name(self) -> str:
        type_name = get_value(self.get_value_type())
        if not isinstance(type_name, str):
            type_name = get_name(type_name)
        return str(type_name)

    def get_type_in(self, dialect: DialectType) -> Union[Type, str]:
        if not isinstance(dialect, DialectType):
            dialect = DialectType.detect(dialect)
        if dialect == DialectType.String:  # if dialect is None or dialect == 'str':
            return self.get_value_type_name()
        else:
            return self.get_value_type().get_type_in(dialect)

    def is_numeric(self) -> bool:
        return self.get_value_type() in (ValueType.Int, ValueType.Float)

    def is_boolean(self) -> bool:
        return self.get_value_type() == ValueType.Bool

    def is_string(self) -> bool:
        return self.get_value_type() == ValueType.Str

    def get_converter(self, source: DialectType, target: DialectType) -> Callable:
        return self.get_value_type().get_converter(source, target)

    def get_example_value(self) -> Any:
        return self._example_value

    def set_example_value(self, value: Any, inplace: bool = True) -> Native:
        if inplace:
            self._example_value = value
            return self
        else:
            field = self.set_outplace(example_value=value)
            return self._assume_native(field)

    def get_default_value(self) -> Any:
        return self._default_value

    def set_default_value(self, value: Any) -> Native:
        self._default_value = value
        return self

    def get_default_item_type(self) -> ItemType:
        return self._default_item_type

    def set_default_item_type(self, item_type: ItemType) -> Native:
        self._default_item_type = item_type
        return self

    @classmethod
    def set_struct_builder(cls, struct_builder: Callable) -> None:
        cls._struct_builder = struct_builder

    @classmethod
    def get_struct_builder(cls, default: Callable = list) -> Callable:
        if cls._struct_builder:
            return cls._struct_builder
        else:
            return default

    def get_sql_expression(self) -> str:
        return self.get_name()

    def get_str_repr(self) -> str:
        return self.get_name()

    def get_brief_repr(self) -> str:
        return '{}: {}'.format(self.get_name(), self.get_value_type_name())

    def format(self, value, skip_errors: bool = False) -> str:
        representation = self.get_representation()
        if Auto.is_defined(representation):
            try:
                return representation.format(value, skip_errors=skip_errors)
            except AttributeError:
                return representation.format(value)
        else:
            return str(value)

    def is_valid(self) -> AutoBool:
        return self._is_valid

    def set_valid(self, is_valid: bool, inplace: bool) -> Native:
        if inplace:
            self._is_valid = is_valid
            return self
        else:
            field = self.make_new(is_valid=is_valid)
            return self._assume_native(field)

    def check_value(self, value) -> bool:
        return self.get_value_type().check_value(value)

    # @deprecated
    def get_group_name(self) -> str:
        return self._group_name

    # @deprecated
    def set_group_name(self, group_name: str, inplace: bool) -> Native:
        if inplace:
            self._group_name = group_name
            return self
        else:
            field = self.make_new(group_name=group_name)
            return self._assume_native(field)

    # @deprecated
    def get_group_caption(self) -> str:
        return self._group_caption

    # @deprecated
    def set_group_caption(self, group_caption: str, inplace: bool) -> Native:
        if inplace:
            self._group_caption = group_caption
            return self
        else:
            field = self.make_new(group_caption=group_caption)
            return self._assume_native(field)

    def get_value_from_item(self, item, item_type: ItemType, struct: Union[Auto, StructInterface] = AUTO) -> Any:
        if item_type == ItemType.Record:
            return item.get(self.get_name())
        else:
            raise ValueError(item_type)

    def to(self, target: Union[str, FieldInterface]) -> ae.AbstractDescription:
        if hasattr(self, '_transform'):
            default_transform = self._transform or (lambda i: i)
        else:
            default_transform = None
        if default_transform:
            return ce.RegularDescription(
                target=target, target_item_type=self.get_default_item_type(),
                inputs=[self], input_item_type=ItemType.Auto,
                function=default_transform, default=self.get_default_value(),
                skip_errors=self._skip_errors, logger=self._logger,
            )
        else:
            return ce.AliasDescription(
                alias=target, target_item_type=self.get_default_item_type(),
                source=self, input_item_type=ItemType.Auto,
                default=self.get_default_value(),
                skip_errors=self._skip_errors, logger=self._logger,
            )

    def as_type(self, field_type: ValueType) -> ce.RegularDescription:
        return ce.RegularDescription(
            target=self.get_name(), target_item_type=self.get_default_item_type(),
            inputs=[self], input_item_type=ItemType.Auto,
            function=field_type.convert, default=self.get_default_value(),
            skip_errors=self._skip_errors, logger=self._logger,
        )

    def filter(self, value_or_func: Union[Callable, Any], tmp_field: Optional[str] = None) -> ce.RegularDescription:
        if isinstance(value_or_func, Callable):
            function = value_or_func
        else:
            value = value_or_func
            function = (lambda v: v == value)
        return ce.RegularDescription(
            target=tmp_field, target_item_type=self.get_default_item_type(),
            inputs=[self], input_item_type=ItemType.Auto,
            function=function, default=None,
            skip_errors=self._skip_errors, logger=self._logger,
        )

    def drop(self):
        return ce.DropDescription([self], target_item_type=ItemType.Auto)

    def get_plural(self, suffix: AutoName = AUTO, caption_prefix: str = 'list of ', **kwargs) -> Native:
        name = self.get_name()
        plural_name = get_plural(name, suffix) if Auto.is_defined(suffix) else get_plural(name)
        meta = self.get_meta()
        meta['name'] = plural_name
        meta['caption'] = caption_prefix + self.get_caption()
        meta['value_type'] = ValueType.Sequence
        meta.update(kwargs)
        field_class = FieldRoleType.Series.get_class()
        return field_class(**meta)

    def get_str_headers(self) -> Generator:
        yield self.get_brief_repr()
        yield from self.get_brief_meta_description()
        yield EMPTY

    def get_count(self) -> int:
        return 1

    def __len__(self):
        return self.get_count()

    def __str__(self):
        return self.get_detailed_repr()

    def __iter__(self):
        yield self.get_name()

    def __add__(self, other: Union[FieldInterface, StructInterface, str]) -> StructInterface:
        struct_builder = self.get_struct_builder()
        field_builder = self.__class__
        if isinstance(other, str):
            return struct_builder([self, field_builder(other)])
        elif isinstance(other, AnyField):
            return struct_builder([self, other])
        elif isinstance(other, ARRAY_TYPES):
            return struct_builder([self] + list(other))
        elif isinstance(other, StructInterface):
            struct = other.append_field(self, before=True, inplace=False)
            assert isinstance(struct, StructInterface), struct
            return struct
        else:
            raise TypeError('Expected other as field or struct, got {} as {}'.format(other, type(other)))

    @staticmethod
    def _assume_native(field) -> Native:
        return field
