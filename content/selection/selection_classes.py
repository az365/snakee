from typing import Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import FieldNo, FieldName, Value
    from base.constants.chars import ALL, NOT_SET
    from base.functions.arguments import get_name, get_name_or_callable
    from utils.decorators import deprecated
    from content.items.item_type import ItemType
    from content.items.item_getters import get_selection_mapper
    from content.fields.field_interface import FieldInterface
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
    from ...base.classes.typing import FieldNo, FieldName, Value
    from ...base.constants.chars import ALL, NOT_SET
    from ...base.functions.arguments import get_name, get_name_or_callable
    from ...utils.decorators import deprecated
    from ..items.item_type import ItemType
    from ..items.item_getters import get_selection_mapper
    from ..fields.field_interface import FieldInterface
    from . import selection_functions as sf
    from .abstract_expression import (
        AbstractDescription, SingleFieldDescription, MultipleFieldDescription, TrivialMultipleDescription,
    )
    from .concrete_expression import (
        TrivialDescription, AliasDescription, RegularDescription, FunctionDescription,
        StarDescription, DropDescription,
    )
    from .selection_description import SelectionDescription, translate_names_to_columns


def get_selection_tuple(description: Union[AbstractDescription, Iterable], or_star: bool = True) -> Union[tuple, str]:
    if isinstance(description, AbstractDescription) or hasattr(description, 'get_selection_tuple'):
        return description.get_selection_tuple(including_target=True)
    elif str(description) == ALL:  # *
        if or_star:
            return ALL  # *
        else:
            return description,
    elif isinstance(description, (FieldNo, FieldName, Callable)):
        return description
    elif isinstance(description, FieldInterface) or hasattr(description, 'get_value_type'):
        return get_name(description)
    elif isinstance(description, Iterable):  # and not isinstance(description, (str, FieldInterface)):
        return tuple([get_name_or_callable(f) for f in description])
    else:
        raise TypeError(f'AbstractDescription or Field expected, got {description}')


def get_compatible_expression_tuples(expressions: dict) -> dict:
    prepared_expressions = dict()
    for k, v in expressions.items():
        name = get_name(k)
        if isinstance(v, (list, tuple)):
            value = get_selection_tuple(v)
        elif isinstance(v, AbstractDescription) or hasattr(v, 'get_selection_tuple'):
            value = v.get_selection_tuple()
        else:
            value = get_name_or_callable(v)
        prepared_expressions[name] = value
    return prepared_expressions


def get_selection_function(
        *fields,
        target_item_type: ItemType = ItemType.Auto,
        input_item_type: ItemType = ItemType.Auto,
        input_struct=None,
        logger=None,
        selection_logger=None,
        use_extended_method=True,
        **expressions
):
    if use_extended_method:
        transform = SelectionDescription.with_expressions(
            fields=list(fields), expressions=expressions,
            input_item_type=input_item_type, target_item_type=target_item_type,
            input_struct=input_struct,
            logger=logger, selection_logger=selection_logger,
        )
        return transform.get_mapper(logger=selection_logger)
    else:
        fields = [get_selection_tuple(f) for f in fields]
        expressions = get_compatible_expression_tuples(expressions)
        return get_selection_mapper(
            *fields, **expressions,
            input_item_type=input_item_type, target_item_type=target_item_type,
            logger=logger, selection_logger=selection_logger,
        )


def get_output_struct(
        *fields,
        target_item_type: ItemType = ItemType.Auto,
        input_item_type: ItemType = ItemType.Auto,
        input_struct=None,
        skip_missing: bool = False,
        logger=None,
        selection_logger=None,
        **expressions
):
    if input_struct:
        transform = SelectionDescription.with_expressions(
            fields=list(fields),
            expressions=expressions,
            target_item_type=target_item_type,
            input_item_type=input_item_type,
            input_struct=input_struct,
            logger=logger,
            selection_logger=selection_logger,
        )
        return transform.get_output_struct()
    elif skip_missing:
        output_fields = list()
        for f in fields:
            if f == ALL or isinstance(f, StarDescription):
                return None
            field_name = get_name(f, or_class=False)
            output_fields.append(field_name)
        return output_fields + list(expressions)


@deprecated
def drop(*fields, **kwargs):
    return DropDescription(fields, **kwargs)


def const(value: Value) -> RegularDescription:
    return RegularDescription(target=NOT_SET, function=lambda i: value, inputs=[], target_item_type=ItemType.Auto)
