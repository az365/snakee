from typing import Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO
    from base.functions.arguments import get_name
    from base.constants.chars import STAR
    from content.items.item_type import ItemType
    from content.items.item_getters import get_selection_mapper
    from content.selection import selection_functions as sf
    from content.selection.abstract_expression import (
        AbstractDescription, SingleFieldDescription, MultipleFieldDescription, TrivialMultipleDescription,
    )
    from content.selection.concrete_expression import (
        TrivialDescription, AliasDescription, RegularDescription, FunctionDescription,
        StarDescription, DropDescription,
    )
    from content.selection.selection_description import SelectionDescription, translate_names_to_columns
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO
    from ...base.functions.arguments import get_name
    from ...base.constants.chars import STAR
    from ..items.item_type import ItemType
    from ..items.item_getters import get_selection_mapper
    from . import selection_functions as sf
    from .abstract_expression import (
        AbstractDescription, SingleFieldDescription, MultipleFieldDescription, TrivialMultipleDescription,
    )
    from .concrete_expression import (
        TrivialDescription, AliasDescription, RegularDescription, FunctionDescription,
        StarDescription, DropDescription,
    )
    from .selection_description import SelectionDescription, translate_names_to_columns



# @deprecated
def is_expression_description(obj) -> bool:  # moved to SelectionDescription
    if isinstance(obj, AbstractDescription):
        return True
    else:
        return hasattr(obj, 'get_selection_tuple')


# @deprecated
def get_name_or_function(field) -> Union[int, str, Callable]:  # not used
    if isinstance(field, Callable):
        return field
    else:
        return get_name(field)


# used in this module only
def get_selection_tuple(description: Union[AbstractDescription, Iterable], or_star: bool = True) -> Union[tuple, str]:
    if isinstance(description, AbstractDescription) or hasattr(description, 'get_selection_tuple'):
        return description.get_selection_tuple(including_target=True)
    elif isinstance(description, Iterable) and not isinstance(description, str):
        return tuple([get_name(f, or_callable=True) for f in description])
    elif str(description) == STAR:
        if or_star:
            return STAR
        else:
            return description,
    else:
        return get_name(description, or_callable=True)


def get_compatible_expression_tuples(expressions: dict) -> dict:  # used in get_selection_function() only
    prepared_expressions = dict()
    for k, v in expressions.items():
        name = get_name(k)
        if isinstance(v, (list, tuple)):
            value = get_selection_tuple(v)
        elif is_expression_description(v):
            value = v.get_selection_tuple()
        else:
            value = get_name(v, or_callable=True)
        prepared_expressions[name] = value
    return prepared_expressions


def get_selection_function(  # used in Any|Row|RecordStream
        *fields,
        target_item_type=ItemType.Auto,
        input_item_type=ItemType.Auto,
        logger=None,
        selection_logger=AUTO,
        use_extended_method=True,
        **expressions
):
    if use_extended_method:
        transform = SelectionDescription.with_expressions(
            fields=list(fields),
            expressions=expressions,
            target_item_type=target_item_type,
            input_item_type=input_item_type,
            logger=logger,
            selection_logger=selection_logger,
        )
        return transform.get_mapper(logger=selection_logger)
    else:
        fields = [get_selection_tuple(f) for f in fields]
        expressions = get_compatible_expression_tuples(expressions)
        return get_selection_mapper(
            *fields,
            target_item_type=target_item_type,
            input_item_type=input_item_type,
            logger=logger,
            selection_logger=selection_logger,
            **expressions,
        )


# @deprecated
def drop(*fields, **kwargs):  # not used
    return DropDescription(fields, **kwargs)
