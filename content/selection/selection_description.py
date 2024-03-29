from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructInterface, LoggerInterface,
        ItemType, Item, Row, Record, Field, Name, FieldName, FieldNo,
        ARRAY_TYPES, Array,
    )
    from base.functions.errors import get_type_err_msg
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.iter_data_mixin import IterDataMixin
    from functions.primary import items as it
    from content.fields.any_field import AnyField
    from content.selection.selection_classes import (
        AbstractDescription, SingleFieldDescription,
        TrivialDescription, StarDescription, AliasDescription, FunctionDescription, RegularDescription,
    )
    from content.selection.selection_functions import topologically_sorted
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructInterface, LoggerInterface,
        ItemType, Item, Row, Record, Field, Name, FieldName, FieldNo,
        ARRAY_TYPES, Array,
    )
    from ...base.functions.errors import get_type_err_msg
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.mixin.iter_data_mixin import IterDataMixin
    from ...functions.primary import items as it
    from ..fields.any_field import AnyField
    from .selection_classes import (
        AbstractDescription, SingleFieldDescription,
        TrivialDescription, StarDescription, AliasDescription, FunctionDescription, RegularDescription,
    )
    from .selection_functions import topologically_sorted

Logger = Optional[LoggerInterface]
Struct = Union[StructInterface, Iterable, None]
Description = AbstractDescription
NAME_TYPES = FieldName, FieldNo
DESC_TYPES = FieldName, FieldNo, Description
FIELD_TYPES = FieldName, FieldNo, AnyField

META_MEMBER_MAPPING = dict(_data='descriptions')
NULL_LOGGER = None


def is_selection_tuple(t) -> bool:
    return t and isinstance(t, ARRAY_TYPES)


def is_expression_description(obj) -> bool:
    if isinstance(obj, AbstractDescription):
        return True
    else:
        return hasattr(obj, 'get_selection_tuple')


def build_expression_description(left: Field, right: Union[Array, Callable, None] = None, **kwargs) -> Description:
    if is_expression_description(left):
        assert right is None
        return left
    elif is_selection_tuple(left):
        assert right is None
        target, desc = left[0], left[1:]
    else:
        target, desc = left, right
    if is_selection_tuple(desc):
        if len(desc) > 1:
            return RegularDescription.from_list(target, list_description=desc, **kwargs)
        else:
            desc = desc[0]
    if isinstance(desc, Callable):
        return FunctionDescription(target, function=desc, **kwargs)
    elif desc is not None:
        assert isinstance(desc, FIELD_TYPES), get_type_err_msg(expected=FIELD_TYPES, got=desc, arg='field')
        return AliasDescription(alias=target, source=desc, **kwargs)
    elif target == '*':
        return StarDescription(**kwargs)
    else:
        return TrivialDescription(target, **kwargs)


def compose_descriptions(
        fields: Iterable,
        expressions: dict,
        target_item_type: ItemType,
        input_item_type: ItemType,
        skip_errors: bool = False,
        logger: Logger = None,
        selection_logger: Logger = None,
) -> Iterable:
    assert isinstance(target_item_type, ItemType) or hasattr(target_item_type, 'get_field_getter'), target_item_type
    target_is_row = target_item_type == ItemType.Row
    kwargs = dict(
        target_item_type=target_item_type, input_item_type=input_item_type,
        skip_errors=skip_errors,
        logger=selection_logger,
    )
    finish_descriptions = list()
    for n, f in enumerate(fields):
        args = (n, f) if target_is_row else (f, )
        cur_desc = build_expression_description(*args, **kwargs)
        if cur_desc.must_finish():
            finish_descriptions.append(cur_desc)
        else:
            yield cur_desc
    ignore_cycles = logger is not None
    for args in topologically_sorted(expressions, ignore_cycles=ignore_cycles, logger=logger):
        yield build_expression_description(*args, **kwargs)
    yield from finish_descriptions


def translate_names_to_columns(expression, struct: StructInterface) -> tuple:
    if isinstance(expression, FieldName):
        return struct.get_field_position(expression),
    elif isinstance(expression, Iterable):
        processed_expression = list()
        for e in expression:
            if isinstance(e, Callable):
                processed_expression.append(e)
            else:
                processed_expression.append(struct.get_field_position(e))
        return tuple(processed_expression)
    else:
        return expression


class SelectionDescription(SimpleDataWrapper, IterDataMixin):
    def __init__(
            self,
            descriptions: Array,
            target_item_type: ItemType = ItemType.Auto,
            input_item_type: ItemType = ItemType.Auto,
            input_struct: Struct = None,
            logger: Logger = None,
            selection_logger: Logger = None,
            name: str = 'select',
            caption: str = '',
    ):
        if not selection_logger:
            selection_logger = getattr(logger, 'get_selection_logger', NULL_LOGGER)
        self._target_item_type = target_item_type
        self._input_item_type = input_item_type
        self._input_struct = input_struct
        self._logger = logger
        self._selection_logger = selection_logger
        self._has_trivial_multiple_selectors = None
        self._output_field_names = None
        super().__init__(data=descriptions, name=name, caption=caption)

    @classmethod
    def with_expressions(
            cls,
            fields: Array,
            expressions: dict,
            target_item_type: ItemType = ItemType.Auto,
            input_item_type: ItemType = ItemType.Auto,
            input_struct: Struct = None,
            skip_errors: bool = True,
            logger: Logger = None,
            selection_logger: Logger = None,
    ):
        descriptions = compose_descriptions(
            fields, expressions,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors,
            logger=logger, selection_logger=selection_logger,
        )
        return cls(
            descriptions=list(descriptions),
            target_item_type=target_item_type, input_item_type=input_item_type,
            input_struct=input_struct,
            logger=logger, selection_logger=selection_logger,
        )

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    def get_descriptions(self) -> Iterable:
        return self._get_data()

    def get_logger(self) -> Logger:
        return self._logger

    def set_logger(
            self,
            logger: Logger = None,
            selection_logger: Logger = None,
    ) -> None:
        if not selection_logger:
            selection_logger = getattr(logger, 'get_selection_logger', NULL_LOGGER)
        self._logger = logger
        self._selection_logger = selection_logger

    def set_selection_logger(self, logger: Logger) -> None:
        self._selection_logger = logger

    def get_selection_logger(self) -> Logger:
        return self._selection_logger

    def get_target_item_type(self) -> ItemType:
        return self._target_item_type

    def get_input_item_type(self) -> ItemType:
        return self._input_item_type

    def get_input_struct(self) -> Struct:
        return self._input_struct

    def get_output_struct(self) -> Struct:
        input_struct = self.get_input_struct()
        if input_struct:
            struct_class = input_struct.__class__
        else:
            raise ValueError('SelectionDescription.get_output_struct(): input struct not set ({})'.format(input_struct))
        output_fields = self.get_output_field_descriptions(input_struct)
        output_struct = struct_class(output_fields)
        return output_struct

    def has_trivial_multiple_selectors(self) -> Optional[bool]:
        return self._has_trivial_multiple_selectors

    def mark_trivial_multiple_selectors(self, value: bool = True) -> None:
        self._has_trivial_multiple_selectors = value

    def check_has_trivial_multiple_selectors(self) -> bool:
        if self.has_trivial_multiple_selectors() is None:
            self.mark_trivial_multiple_selectors(False)
            for d in self.get_descriptions():
                if hasattr(d, 'is_trivial_multiple'):
                    if d.is_trivial_multiple():
                        self.mark_trivial_multiple_selectors(True)
                        break
        return self.has_trivial_multiple_selectors()

    def check_changing_output_fields(self) -> bool:
        return self.check_has_trivial_multiple_selectors() and self.get_input_item_type() == ItemType.Record

    def get_known_output_field_names(self) -> list:
        return self._output_field_names

    def add_output_field_names(self, item_or_struct: Union[Item, Struct]) -> None:
        for d in self.get_descriptions():
            for f in d.get_output_field_names(item_or_struct):
                if f not in self._output_field_names:
                    self._output_field_names.append(f)

    def reset_output_field_names(self, item: Item, inplace: bool = True):
        self._output_field_names = list()
        for d in self.get_descriptions():
            added_fields = d.get_output_field_names(item)
            self._output_field_names += list(added_fields)
        if not inplace:
            return self

    def get_output_field_names(self, item: Optional[Item] = None) -> list:
        if item is not None:
            if self.check_changing_output_fields() or self.get_known_output_field_names() is None:
                self.reset_output_field_names(item, inplace=True)
        return self.get_known_output_field_names()

    def get_dict_output_field_types(self, struct: Struct = None) -> dict:
        if struct is None:
            struct = self.get_input_struct()
        output_types = dict()
        for d in self.get_descriptions():
            output_types.update(d.get_dict_output_field_types(struct))
        return output_types

    def get_output_field_descriptions(self, struct: Struct = None) -> Iterable:
        dict_output_field_types = self.get_dict_output_field_types(struct)
        for name in self.get_output_field_names(struct):
            value_type = dict_output_field_types.get(name)
            yield AnyField(name, value_type=value_type)

    def select_output_fields(self, item: Item) -> Item:
        return it.simple_select_fields(
            fields=self.get_output_field_names(item),
            item=item,
            item_type=self.get_target_item_type(),
        )

    def apply_inplace(self, item: Item) -> None:
        for d in self.get_descriptions():
            d.apply_inplace(item)

    def apply_outplace(self, item: Item, target_item_type: ItemType) -> Item:
        output_item = target_item_type.build()
        for d in self.get_descriptions():
            assert isinstance(d, SingleFieldDescription) or hasattr(d, 'get_target_field_name'), f'got {d}'
            it.set_to_item_inplace(
                field=d.get_target_field_name(),
                value=d.get_value_from_item(item),
                item=output_item,
                item_type=target_item_type,
            )
        return output_item

    def process_item(self, item: Item) -> Item:
        input_item_type = self.get_input_item_type()
        target_item_type = self.get_target_item_type()
        if input_item_type in (ItemType.Auto, None):
            input_item_type = ItemType.detect(item, default=ItemType.Any)
        if target_item_type in (ItemType.Auto, None):
            target_item_type = input_item_type
        if target_item_type == input_item_type and target_item_type != ItemType.Row:
            new_item = it.get_copy(item)
            self.apply_inplace(new_item)
            new_item = self.select_output_fields(new_item)
        else:
            new_item = self.apply_outplace(item, target_item_type)
        return it.get_frozen(new_item)

    def get_mapper(self, logger: Logger = None) -> Callable:
        if logger:
            self.set_selection_logger(logger)
        return self.process_item

    def __repr__(self):
        return str(self)

    def __str__(self):
        class_name = self.__class__.__name__
        descriptions = self.get_descriptions()
        return f'{class_name}({descriptions})'
