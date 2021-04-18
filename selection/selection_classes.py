from typing import Iterable

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from selection.abstract_expression import (
        AbstractDescription, SingleFieldDescription, MultipleFieldDescription, TrivialMultipleDescription,
    )
    from selection.concrete_expression import (
        TrivialDescription, AliasDescription, RegularDescription, FunctionDescription,
        StarDescription, DropDescription,
    )
    from selection.abstract_expression import AbstractDescription, SingleFieldDescription, MultipleFieldDescription
    from selection.selection_description import SelectionDescription
    from utils import items as it
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from .abstract_expression import (
        AbstractDescription, SingleFieldDescription, MultipleFieldDescription, TrivialMultipleDescription,
    )
    from .concrete_expression import (
        TrivialDescription, AliasDescription, RegularDescription, FunctionDescription,
        StarDescription, DropDescription,
    )
    from .selection_description import SelectionDescription


def _prepare_field_list(field_list: Iterable) -> tuple:
    prepared_field_list = list()
    for f in field_list:
        if hasattr(f, 'get_name'):
            name = f.get_name()
        else:
            name = f
        prepared_field_list.append(name)
    return tuple(prepared_field_list)


def _prepare_expressions(expressions: dict) -> dict:
    prepared_expressions = dict()
    for k, v in expressions.items():
        if hasattr(k, 'get_name'):
            name = k.get_name()
        else:
            name = k
        if hasattr(v, 'get_name'):
            value = v.get_name()
        elif isinstance(v, str):
            value = v
        elif isinstance(v, Iterable):
            value = _prepare_field_list(v)
        else:
            value = v
        prepared_expressions[name] = value
    return prepared_expressions


def select(
        *fields,
        target_item_type=it.ItemType.Auto, input_item_type=it.ItemType.Auto,
        logger=None, selection_logger=arg.DEFAULT,
        use_extended_method=True,
        **expressions
):
    if use_extended_method:
        return SelectionDescription.with_expressions(
            fields=list(fields),
            expressions=expressions,
            target_item_type=target_item_type,
            input_item_type=input_item_type,
            logger=logger,
            selection_logger=selection_logger,
        ).get_mapper(
            logger=selection_logger,
        )
    else:
        fields = _prepare_field_list(fields)
        expressions = _prepare_expressions(expressions)
        return sf.select(
            *fields,
            target_item_type=target_item_type,
            input_item_type=input_item_type,
            logger=logger,
            selection_logger=selection_logger,
            **expressions,
        )


def drop(*fields, **kwargs):
    return DropDescription(fields, **kwargs)


def is_expression_description(obj):
    return isinstance(obj, AbstractDescription)
