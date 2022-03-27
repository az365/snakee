from typing import Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO
    from base.functions.arguments import get_name
    from utils import selection as sf
    from content.items.item_type import ItemType
    from content.selection.abstract_expression import (
        AbstractDescription, SingleFieldDescription, MultipleFieldDescription, TrivialMultipleDescription,
    )
    from content.selection.concrete_expression import (
        TrivialDescription, AliasDescription, RegularDescription, FunctionDescription,
        StarDescription, DropDescription,
    )
    from content.selection.abstract_expression import AbstractDescription, SingleFieldDescription, MultipleFieldDescription
    from content.selection.selection_description import SelectionDescription, translate_names_to_columns
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO
    from ...base.functions.arguments import get_name
    from ...utils import selection as sf
    from ..items.item_type import ItemType
    from .abstract_expression import (
        AbstractDescription, SingleFieldDescription, MultipleFieldDescription, TrivialMultipleDescription,
    )
    from .concrete_expression import (
        TrivialDescription, AliasDescription, RegularDescription, FunctionDescription,
        StarDescription, DropDescription,
    )
    from .selection_description import SelectionDescription, translate_names_to_columns

STAR = '*'


def is_expression_description(obj) -> bool:
    if isinstance(obj, AbstractDescription):
        return True
    else:
        return hasattr(obj, 'get_selection_tuple')


def get_name_or_function(field) -> Union[int, str, Callable]:
    if isinstance(field, Callable):
        return field
    else:
        return get_name(field)


def get_selection_tuple(description: Union[AbstractDescription, Iterable], or_star: bool = True) -> Union[tuple, str]:
    if isinstance(description, AbstractDescription) or hasattr(description, 'get_selection_tuple'):
        return description.get_selection_tuple(including_target=True)
    elif isinstance(description, Iterable) and not isinstance(description, str):
        return tuple([get_name_or_function(f) for f in description])
    elif str(description) == STAR:
        if or_star:
            return STAR
        else:
            return description,
    else:
        return get_name_or_function(description)


def get_compatible_expression_tuples(expressions: dict) -> dict:
    prepared_expressions = dict()
    for k, v in expressions.items():
        name = get_name(k)
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
        target_item_type=ItemType.Auto,
        input_item_type=ItemType.Auto,
        logger=None,
        selection_logger=AUTO,
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
        fields = [get_selection_tuple(f) for f in fields]
        expressions = get_compatible_expression_tuples(expressions)
        return sf.get_selection_mapper(
            *fields,
            target_item_type=target_item_type,
            input_item_type=input_item_type,
            logger=logger,
            selection_logger=selection_logger,
            **expressions,
        )


def drop(*fields, **kwargs):
    return DropDescription(fields, **kwargs)
