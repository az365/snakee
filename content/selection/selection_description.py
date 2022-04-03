from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructInterface, LoggerInterface,
        ItemType, Item, Row, Record, Field, Name, Array, ARRAY_TYPES,
        AUTO, Auto,
    )
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.data_mixin import IterDataMixin
    from utils import selection as sf
    from content.fields.simple_field import SimpleField
    from functions.primary import items as it
    from content.selection import selection_classes as sn
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructInterface, LoggerInterface,
        ItemType, Item, Row, Record, Field, Name, Array, ARRAY_TYPES,
        AUTO, Auto,
    )
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.mixin.data_mixin import IterDataMixin
    from ...utils import selection as sf
    from ..fields.simple_field import SimpleField
    from ...functions.primary import items as it
    from . import selection_classes as sn

Logger = Optional[LoggerInterface]
Struct = Union[Optional[StructInterface], Iterable]
Description = sn.AbstractDescription
NAME_TYPES = int, str
DESC_TYPES = int, str, Description

META_MEMBER_MAPPING = dict(_data='descriptions')


def is_selection_tuple(t) -> bool:
    return t and isinstance(t, ARRAY_TYPES)


def build_expression_description(left: Field, right: Union[Optional[Array], Callable] = None, **kwargs) -> Description:
    if sn.is_expression_description(left):
        assert right is None
        return left
    elif is_selection_tuple(left):
        assert right is None
        target, desc = left[0], left[1:]
    else:
        target, desc = left, right
    if is_selection_tuple(desc):
        if len(desc) > 1:
            return sn.RegularDescription.from_list(target, list_description=desc, **kwargs)
        else:
            desc = desc[0]
    if isinstance(desc, Callable):
        return sn.FunctionDescription(target, function=desc, **kwargs)
    elif desc is not None:
        assert isinstance(desc, (int, str)), 'int or str expected, got {}'.format(desc)
        return sn.AliasDescription(alias=target, source=desc, **kwargs)
    elif target == '*':
        return sn.StarDescription(**kwargs)
    else:
        return sn.TrivialDescription(target, **kwargs)


def compose_descriptions(
        fields: Iterable,
        expressions: dict,
        target_item_type: ItemType,
        input_item_type: ItemType,
        skip_errors: bool = False,
        logger: Logger = None,
        selection_logger: Logger = None,
) -> Iterable:
    assert isinstance(target_item_type, ItemType)
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
    for args in sf.topologically_sorted(expressions, ignore_cycles=ignore_cycles, logger=logger):
        yield build_expression_description(*args, **kwargs)
    yield from finish_descriptions


def translate_names_to_columns(expression, struct: StructInterface) -> tuple:
    if isinstance(expression, str):
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
            selection_logger: Union[Logger, Auto] = AUTO,
            name: str = 'select',
            caption: str = '',
    ):
        self._target_item_type = target_item_type
        self._input_item_type = input_item_type
        self._input_struct = input_struct
        self._logger = logger
        self._selection_logger = Auto.acquire(selection_logger, getattr(logger, 'get_selection_logger', None))
        self._has_trivial_multiple_selectors = AUTO
        self._output_field_names = AUTO
        super().__init__(data=descriptions, name=name, caption=caption)

    @classmethod
    def with_expressions(
            cls, fields: Array, expressions: dict,
            target_item_type: ItemType = ItemType.Auto, input_item_type: ItemType = ItemType.Auto,
            input_struct=None, skip_errors=True,
            logger=None, selection_logger=AUTO,
    ):
        descriptions = compose_descriptions(
            fields, expressions,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors,
            logger=logger, selection_logger=selection_logger,
        )
        if input_item_type == ItemType.StructRow and input_struct:
            descriptions = [translate_names_to_columns(i, struct=input_struct) for i in descriptions]
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
            logger: Union[Logger, Auto] = AUTO,
            selection_logger: Union[Logger, Auto] = AUTO,
    ) -> None:
        self._logger = Auto.acquire(logger, getattr(logger, 'get_logger', None))
        self._selection_logger = Auto.acquire(selection_logger, getattr(logger, 'get_selection_logger', None))

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

    def has_trivial_multiple_selectors(self) -> Union[bool, Auto]:
        return self._has_trivial_multiple_selectors

    def mark_trivial_multiple_selectors(self, value: bool = True) -> None:
        self._has_trivial_multiple_selectors = value

    def check_has_trivial_multiple_selectors(self) -> bool:
        if self.has_trivial_multiple_selectors() == AUTO:
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
        if Auto.is_defined(item, check_name=False):
            if self.check_changing_output_fields() or self.get_known_output_field_names() == AUTO:
                self.reset_output_field_names(item, inplace=True)
        return self.get_known_output_field_names()

    def get_dict_output_field_types(self, struct: Union[Struct, Auto] = AUTO) -> dict:
        struct = Auto.delayed_acquire(struct, self.get_input_struct)
        output_types = dict()
        for d in self.get_descriptions():
            output_types.update(d.get_dict_output_field_types(struct))
        return output_types

    def get_output_field_descriptions(self, struct: Union[Struct, Auto] = AUTO) -> Iterable:
        dict_output_field_types = self.get_dict_output_field_types(struct)
        for name in self.get_output_field_names(struct):
            field_type = dict_output_field_types.get(name)
            yield SimpleField(name, field_type=field_type)

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
            assert isinstance(d, sn.SingleFieldDescription)
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
        if input_item_type == AUTO:
            input_item_type = ItemType.detect(item, default=ItemType.Any)
        if target_item_type == AUTO:
            target_item_type = input_item_type
        if target_item_type == input_item_type and target_item_type != ItemType.Row:
            new_item = it.get_copy(item)
            self.apply_inplace(new_item)
            new_item = self.select_output_fields(new_item)
        else:
            new_item = self.apply_outplace(item, target_item_type)
        return it.get_frozen(new_item)

    def get_mapper(self, logger: Union[Logger, Auto] = AUTO) -> Callable:
        if Auto.is_defined(logger):
            self.set_selection_logger(logger)
        return self.process_item

    def __repr__(self):
        return str(self)

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, self.get_descriptions())
