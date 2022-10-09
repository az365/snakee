from typing import Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.classes.typing import PRIMITIVE_TYPES, Array
    from base.functions.arguments import get_name, get_names, update
    from loggers.logger_interface import LoggerInterface
    from functions.primary.items import ALL, get_field_value_from_item, get_fields_values_from_item
    from utils import algo
    from utils.decorators import deprecated_with_alternative
    from content.items.item_type import ItemType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...base.classes.typing import PRIMITIVE_TYPES, Array
    from ...base.functions.arguments import get_name, get_names, update
    from ...loggers.logger_interface import LoggerInterface
    from ...functions.primary.items import ALL, get_field_value_from_item, get_fields_values_from_item
    from ...utils import algo
    from ...utils.decorators import deprecated_with_alternative
    from ..items.item_type import ItemType

Description = Union[Callable, Array]

IGNORE_CYCLIC_DEPENDENCIES = False


def process_description(d) -> tuple:
    if d is None:
        raise ValueError(f'got empty description: {d}')
    if isinstance(d, Callable):
        function, inputs = d, list()
    elif isinstance(d, (list, tuple)):
        if isinstance(d[0], Callable):
            function, inputs = d[0], d[1:]
        elif isinstance(d[-1], Callable):
            inputs, function = d[:-1], d[-1]
        else:
            inputs, function = d, lambda *a: tuple(a)
    else:
        inputs, function = [d], lambda v: v
    return function, get_names(inputs)


def topologically_sorted(expressions: dict, ignore_cycles: bool = IGNORE_CYCLIC_DEPENDENCIES, logger=None) -> list:
    unordered_fields = list()
    unresolved_dependencies = dict()
    for field, description in expressions.items():
        unordered_fields.append(field)
        _, dependencies = process_description(description)
        unresolved_dependencies[field] = [
            d for d in dependencies
            if d in expressions.keys() and d != field
        ]
    ordered_fields = algo.topologically_sorted(
        nodes=unordered_fields,
        edges=unresolved_dependencies,
        ignore_cycles=ignore_cycles,
        logger=logger,
    )
    return [(f, expressions[f]) for f in ordered_fields]


def flatten_descriptions(*fields, to_names: bool = True, **expressions) -> list:
    descriptions = list(fields)
    logger = expressions.pop('logger', None)
    ignore_cycles = logger is not None
    for k, v in topologically_sorted(expressions, ignore_cycles=ignore_cycles, logger=logger):
        if isinstance(v, list):
            descriptions.append([k] + v)
        elif isinstance(v, tuple):
            descriptions.append([k] + list(v))
        else:
            descriptions.append([k] + [v])
    if to_names:
        result = list()
        for desc in descriptions:
            if isinstance(desc, (list, tuple)):
                desc = get_names(desc, or_callable=True)
            result.append(desc)
        return result
    else:
        return descriptions


def safe_apply_function(function: Callable, fields, values, item=None, logger=None, skip_errors=True) -> Any:
    item = item or dict()
    try:
        return function(*values)
    except TypeError or ValueError as e:
        if logger:
            if hasattr(logger, 'log_selection_error'):
                logger.log_selection_error(function, fields, values, item, e)
            else:
                level = 30 if skip_errors else 40
                message = 'Error while processing function {} over fields {} with values {}.'
                func_name = get_name(function, or_callable=False)
                logger.log(msg=message.format(func_name, fields, values), level=level)
        if not skip_errors:
            raise e


def support_simple_filter_expressions(*fields, **expressions) -> list:
    extended_filter_list = list()
    for desc in flatten_descriptions(*fields, **expressions):
        new_desc = desc
        if isinstance(desc, (list, tuple)):
            if len(desc) == 2:
                name, value = desc
                if isinstance(value, PRIMITIVE_TYPES):
                    new_desc = name, lambda i, v=value: i == v
        extended_filter_list.append(new_desc)
    return extended_filter_list
