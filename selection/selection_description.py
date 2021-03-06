from typing import Optional, Callable, Iterable, Union, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        selection as sf,
        items as it,
    )
    from items.base_item_type import ItemType
    from items.struct_row_interface import StructRowInterface
    from fields.schema_interface import SchemaInterface
    from loggers.logger_interface import LoggerInterface
    from selection import selection_classes as sn
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        selection as sf,
        items as it,
    )
    from ..items.base_item_type import ItemType
    from ..items.struct_row_interface import StructRowInterface
    from ..fields.schema_interface import SchemaInterface
    from ..loggers.logger_interface import LoggerInterface
    from . import selection_classes as sn
    from ..schema import schema_classes as sh

Row = Union[list, tuple]
Record = dict
Item = Union[StructRowInterface, Row, Record]
Logger = Optional[LoggerInterface]
Schema = Union[Optional[SchemaInterface], Iterable]
Description = sn.AbstractDescription
Name = Union[int, str]
Field = Union[Name, Description]
Array = Union[list, tuple]
ARRAY_TYPES = list, tuple
NAME_TYPES = int, str
DESC_TYPES = int, str, Description


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
        assert isinstance(desc, (int, str))
        return sn.AliasDescription(alias=target, source=desc, **kwargs)
    elif target == '*':
        return sn.StarDescription(**kwargs)
    else:
        return sn.TrivialDescription(target, **kwargs)


def compose_descriptions(
        fields: Iterable, expressions: dict,
        target_item_type: ItemType, input_item_type: ItemType,
        skip_errors: bool = False,
        logger: Logger = None, selection_logger: Logger = None,
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


class SelectionDescription:
    def __init__(
            self,
            descriptions: Array,
            target_item_type: ItemType = ItemType.Auto,
            input_item_type: ItemType = ItemType.Auto,
            input_schema: Schema = None,
            logger: Logger = None,
            selection_logger: Union[Logger, arg.DefaultArgument] = arg.DEFAULT,
    ):
        self._descriptions = descriptions
        self._target_item_type = target_item_type
        self._input_item_type = input_item_type
        self._input_schema = input_schema
        self._logger = logger
        self._selection_logger = arg.undefault(selection_logger, getattr(logger, 'get_selection_logger', None))
        self._has_trivial_multiple_selectors = arg.DEFAULT
        self._output_field_names = arg.DEFAULT

    @classmethod
    def with_expressions(
            cls, fields: list, expressions: dict,
            target_item_type: ItemType = ItemType.Auto, input_item_type: ItemType = ItemType.Auto,
            input_schema=None, skip_errors=True,
            logger=None, selection_logger=arg.DEFAULT,
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
            input_schema=input_schema,
            logger=logger, selection_logger=selection_logger,
        )

    def get_descriptions(self) -> Iterable:
        return self._descriptions

    def get_logger(self) -> Logger:
        return self._logger

    def set_logger(
            self,
            logger: Union[Logger, arg.DefaultArgument] = arg.DEFAULT,
            selection_logger: Union[Logger, arg.DefaultArgument] = arg.DEFAULT,
    ) -> NoReturn:
        self._logger = arg.undefault(logger, getattr(logger, 'get_logger', None))
        self._selection_logger = arg.undefault(selection_logger, getattr(logger, 'get_selection_logger', None))

    def set_selection_logger(self, logger: Logger) -> NoReturn:
        self._selection_logger = logger

    def get_selection_logger(self) -> Logger:
        return self._selection_logger

    def get_target_item_type(self) -> ItemType:
        return self._target_item_type

    def get_input_item_type(self) -> ItemType:
        return self._input_item_type

    def get_input_schema(self) -> Schema:
        return self._input_schema

    def get_output_schema(self) -> Schema:
        assert self.get_input_schema()
        return sh.SchemaDescription(
            self.get_output_field_descriptions(
                self.get_input_schema(),
            ),
        )

    def has_trivial_multiple_selectors(self) -> Union[bool, arg.DefaultArgument]:
        return self._has_trivial_multiple_selectors

    def mark_trivial_multiple_selectors(self, value: bool = True) -> NoReturn:
        self._has_trivial_multiple_selectors = value

    def check_has_trivial_multiple_selectors(self) -> bool:
        if self.has_trivial_multiple_selectors() == arg.DEFAULT:
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

    def add_output_field_names(self, item_or_schema: Union[Item, Schema]) -> NoReturn:
        for d in self.get_descriptions():
            for f in d.get_output_field_names(item_or_schema):
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
        if arg.is_defined(item):
            if self.check_changing_output_fields() or self.get_known_output_field_names() == arg.DEFAULT:
                self.reset_output_field_names(item)
        return self.get_known_output_field_names()

    def get_dict_output_field_types(self, schema: Union[Schema, arg.DefaultArgument] = arg.DEFAULT) -> dict:
        schema = arg.delayed_undefault(schema, self.get_input_schema)
        output_types = dict()
        for d in self.get_descriptions():
            output_types.update(d.get_dict_output_field_types(schema))
        return output_types

    def get_output_field_descriptions(self, schema: Union[Schema, arg.DefaultArgument] = arg.DEFAULT) -> Iterable:
        dict_output_field_types = self.get_dict_output_field_types(schema)
        for name in self.get_output_field_names(schema):
            field_type = dict_output_field_types.get(name)
            yield sh.FieldDescription(name, field_type=field_type)

    def select_output_fields(self, item: Item) -> Item:
        return it.simple_select_fields(
            fields=self.get_output_field_names(item),
            item=item,
            item_type=self.get_target_item_type(),
        )

    def apply_inplace(self, item: Item) -> NoReturn:
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
        if input_item_type == arg.DEFAULT:
            input_item_type = ItemType.detect(item, default=ItemType.Any)
        if target_item_type == arg.DEFAULT:
            target_item_type = input_item_type
        if target_item_type == input_item_type and target_item_type != ItemType.Row:
            new_item = it.get_copy(item)
            self.apply_inplace(new_item)
            new_item = self.select_output_fields(new_item)
        else:
            new_item = self.apply_outplace(item, target_item_type)
        return it.get_frozen(new_item)

    def get_mapper(self, logger: Union[Logger, arg.DefaultArgument] = arg.DEFAULT) -> Callable:
        if arg.is_defined(logger):
            self.set_selection_logger(logger)
        return self.process_item
