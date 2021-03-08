from typing import Union

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from selection import selection_classes as sn
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from . import selection_classes as sn
    from ..schema import schema_classes as sh


def build_expression_description(left, right=None, **kwargs):
    if sn.is_expression_description(left):
        assert right is None
        return left
    elif isinstance(left, (list, tuple)):
        assert right is None
        target, desc = left[0], left[1:]
    else:
        target, desc = left, right
    if isinstance(desc, (list, tuple)):
        if len(desc) > 1:
            return sn.RegularDescription.from_list(target, list_description=desc, **kwargs)
        else:
            desc = desc[0]
    if callable(desc):
        return sn.FunctionDescription(target, function=desc, **kwargs)
    elif desc is not None:
        assert isinstance(desc, (int, str))
        return sn.AliasDescription(alias=target, source=desc, **kwargs)
    elif target == '*':
        return sn.StarDescription(**kwargs)
    else:
        return sn.TrivialDescription(target, **kwargs)


def compose_descriptions(
        fields, expressions,
        target_item_type: it.ItemType, input_item_type: it.ItemType,
        skip_errors=False,
        logger=None, selection_logger=None,
):
    assert isinstance(target_item_type, it.ItemType)
    target_is_row = target_item_type == it.ItemType.Row
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
            descriptions: Union[tuple, list],
            target_item_type: it.ItemType = it.ItemType.Auto,
            input_item_type: it.ItemType = it.ItemType.Auto,
            input_schema=None,
            logger=None, selection_logger=arg.DEFAULT,
    ):
        self.descriptions = descriptions
        self.target_item_type = target_item_type
        self.input_item_type = input_item_type
        self.input_schema = input_schema
        self.logger = logger
        self.selection_logger = arg.undefault(selection_logger, getattr(logger, 'get_selection_logger', None))
        self.has_trivial_multiple_selectors = arg.DEFAULT
        self.output_field_names = arg.DEFAULT

    @classmethod
    def with_expressions(
            cls, fields: list, expressions: dict,
            target_item_type=it.ItemType.Auto, input_item_type=it.ItemType.Auto,
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

    def get_descriptions(self):
        return self.descriptions

    def get_logger(self):
        return self.logger

    def set_logger(self, logger=arg.DEFAULT, selection_logger=arg.DEFAULT):
        self.logger = arg.undefault(logger, getattr(logger, 'get_logger', None))
        self.selection_logger = arg.undefault(selection_logger, getattr(logger, 'get_selection_logger', None))

    def get_selection_logger(self):
        return self.selection_logger

    def get_target_item_type(self):
        return self.target_item_type

    def get_input_item_type(self):
        return self.input_item_type

    def get_input_schema(self):
        return self.input_schema

    def get_output_schema(self):
        assert self.get_input_schema()
        return sh.SchemaDescription(
            self.get_output_field_descriptions(
                self.get_input_schema(),
            ),
        )

    def check_has_trivial_multiple_selectors(self):
        if self.has_trivial_multiple_selectors == arg.DEFAULT:
            self.has_trivial_multiple_selectors = False
            for d in self.descriptions:
                if hasattr(d, 'is_trivial_multiple'):
                    if d.is_trivial_multiple():
                        self.has_trivial_multiple_selectors = True
                        break
        return self.has_trivial_multiple_selectors

    def check_changing_output_fields(self):
        return self.check_has_trivial_multiple_selectors() and self.get_input_item_type() == it.ItemType.Record

    def reset_output_field_names(self, item):
        self.output_field_names = list()
        for d in self.get_descriptions():
            added_fields = d.get_output_field_names(item)
            self.output_field_names += list(added_fields)

    def get_output_field_names(self, item):
        if self.check_changing_output_fields() or self.output_field_names == arg.DEFAULT:
            self.reset_output_field_names(item)
        return self.output_field_names

    def get_dict_output_field_types(self, schema=arg.DEFAULT):
        schema = arg.undefault(schema, self.get_input_schema())
        output_types = dict()
        for d in self.get_descriptions():
            output_types.update(d.get_dict_output_field_types(schema))
        return output_types

    def get_output_field_descriptions(self, schema):
        dict_output_field_types = self.get_dict_output_field_types(schema)
        for name in self.get_output_field_names(schema):
            field_type = dict_output_field_types.get(name)
            yield sh.FieldDescription(name, field_type=field_type)

    def select_output_fields(self, item):
        return it.simple_select_fields(
            fields=self.get_output_field_names(item),
            item=item,
            item_type=self.get_target_item_type(),
        )

    def apply_inplace(self, item):
        for d in self.get_descriptions():
            assert isinstance(d, sn.AbstractDescription)
            d.apply_inplace(item)

    def apply_outplace(self, item, target_item_type: it.ItemType):
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

    def process_item(self, item):
        input_item_type = self.get_input_item_type()
        target_item_type = self.get_target_item_type()
        if input_item_type == arg.DEFAULT:
            input_item_type = it.ItemType.detect(item)
        if target_item_type == arg.DEFAULT:
            target_item_type = input_item_type
        if target_item_type == input_item_type and target_item_type != it.ItemType.Row:
            new_item = it.get_copy(item)
            self.apply_inplace(new_item)
            new_item = self.select_output_fields(new_item)
        else:
            new_item = self.apply_outplace(item, target_item_type)
        return it.get_frozen(new_item)

    def get_mapper(self, logger=arg.DEFAULT):
        self.selection_logger = arg.undefault(logger, self.get_selection_logger())
        return self.process_item
