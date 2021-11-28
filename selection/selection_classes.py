from typing import Iterable, Callable, Union

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
    from selection.selection_description import SelectionDescription, translate_names_to_columns
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
    from .selection_description import SelectionDescription, translate_names_to_columns


def is_expression_description(obj) -> bool:
    if isinstance(obj, AbstractDescription):
        return True
    else:
        return hasattr(obj, 'get_selection_tuple')


def get_name_or_function(field) -> Union[int, str, Callable]:
    if isinstance(field, Callable):
        return field
    else:
        return arg.get_name(field)


def get_selection_tuple(description: Union[AbstractDescription, Iterable]) -> tuple:
    if isinstance(description, AbstractDescription):
        return description.get_selection_tuple()
    elif isinstance(description, Iterable):
        return tuple([get_name_or_function(f) for f in description])
    else:
        raise TypeError


def get_compatible_expression_tuples(expressions: dict) -> dict:
    prepared_expressions = dict()
    for k, v in expressions.items():
        name = arg.get_name(k)
        if isinstance(v, (list, tuple)):
            value = get_selection_tuple(v)
        elif is_expression_description(v):
            value = v.get_selection_tuple()
        else:
            value = get_name_or_function(v)
        prepared_expressions[name] = value
    return prepared_expressions


def select(
        *fields,
        target_item_type=it.ItemType.Auto, input_item_type=it.ItemType.Auto,
        logger=None, selection_logger=arg.AUTO,
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
        fields = get_selection_tuple(fields)
        expressions = get_compatible_expression_tuples(expressions)
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
