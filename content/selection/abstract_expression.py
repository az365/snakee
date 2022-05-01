from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructRowInterface, StructInterface, LoggerInterface, LoggingLevel,
        ItemType, Item, Struct, UniKey, FieldInterface, FieldName, FieldNo, Field, Name, Value, Class, Array,
        AUTO, Auto,
    )
    from base.functions.arguments import get_name, get_names
    from base.constants.chars import ITEMS_DELIMITER, CROP_SUFFIX, NOT_SET, DEFAULT_LINE_LEN
    from base.abstract.abstract_base import AbstractBaseObject
    from base.mixin.display_mixin import DisplayMixin
    from loggers.fallback_logger import FallbackLogger
    from functions.primary import items as it
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructRowInterface, StructInterface, LoggerInterface, LoggingLevel,
        ItemType, Item, Struct, UniKey, FieldInterface, FieldName, FieldNo, Field, Name, Value, Class, Array,
        AUTO, Auto,
    )
    from ...base.functions.arguments import get_name, get_names
    from ...base.constants.chars import ITEMS_DELIMITER, CROP_SUFFIX, NOT_SET, DEFAULT_LINE_LEN
    from ...base.abstract.abstract_base import AbstractBaseObject
    from ...base.mixin.display_mixin import DisplayMixin
    from ...loggers.fallback_logger import FallbackLogger
    from ...functions.primary import items as it

SQL_FUNC_NAMES_DICT = dict(len='COUNT')
SQL_TYPE_NAMES_DICT = dict(int='INTEGER')
ALIAS_FUNCTION = '_same'


class AbstractDescription(AbstractBaseObject, DisplayMixin, ABC):
    def __init__(
            self,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
    ):
        self._target_type = target_item_type
        self._input_type = input_item_type
        self._skip_errors = skip_errors
        self._logger = logger

    def get_target_item_type(self) -> ItemType:
        return self._target_type

    def get_input_item_type(self) -> ItemType:
        return self._input_type

    def must_skip_errors(self) -> bool:
        return self._skip_errors

    def get_logger(self) -> LoggerInterface:
        return self._logger

    @abstractmethod
    def get_function(self) -> Callable:
        pass

    @abstractmethod
    def must_finish(self) -> bool:
        pass

    @abstractmethod
    def get_input_fields(self, *args) -> Array:
        pass

    @abstractmethod
    def get_output_fields(self, *args) -> Array:
        pass

    def get_linked_fields(self) -> Array:
        return list(self.get_input_fields()) + list(self.get_output_fields())

    def get_input_field_names(self) -> list:
        return get_names(self.get_input_fields())

    def get_output_field_names(self, *args) -> list:
        return get_names(self.get_output_fields(*args))

    def get_target_field_name(self) -> Optional[str]:
        if hasattr(self, 'get_target_field'):
            target_field = self.get_target_field()
            if target_field:
                return get_name(target_field)

    @abstractmethod
    def get_output_field_types(self, struct: StructInterface) -> Iterable:
        pass

    def get_dict_output_field_types(self, struct: StructInterface) -> dict:
        names = self.get_output_field_names(struct)
        types = self.get_output_field_types(struct)
        return dict(zip(names, types))

    def has_data(self) -> bool:
        return bool(self.get_input_fields()) or bool(self.get_output_fields())

    def get_selection_tuple(self, including_target: bool = False) -> tuple:
        if including_target:
            return (self.get_target_field_name(), self.get_function(), *self.get_input_field_names())
        else:
            return (self.get_function(), *self.get_input_field_names())

    def get_mapper(self, struct: Struct = None, item_type: Union[ItemType, Auto] = AUTO) -> Callable:
        field_names = self.get_input_field_names()
        item_type = Auto.delayed_acquire(item_type, self.get_input_item_type)
        if Auto.is_defined(item_type):
            return item_type.get_single_mapper(*field_names, function=self.get_function(), struct=struct)
        else:
            raise ValueError('item_type must be defined')

    def get_sql_expression(self) -> str:
        function = self.get_function()
        target_field = self.get_target_field_name()
        input_fields = self.get_input_field_names()
        if hasattr(function, 'get_sql_expr'):
            sql_function_expr = function.get_sql_expr(*input_fields)
        else:
            function_name = function.__name__
            sql_type_name = SQL_TYPE_NAMES_DICT.get(function_name)
            if sql_type_name:
                assert len(input_fields) == 1, 'got {}'.format(input_fields)
                sql_function_expr = '{field}::{type}'.format(field=input_fields[0], type=sql_type_name)
            elif function_name == ALIAS_FUNCTION:
                assert len(input_fields) == 1, 'got {}'.format(input_fields)
                sql_function_expr = input_fields[0]
            else:
                sql_function_name = SQL_FUNC_NAMES_DICT.get(function_name)
                if not sql_function_name:
                    self.warning('Unsupported function call: {}'.format(function_name))
                    sql_function_name = function_name
                input_fields = ITEMS_DELIMITER.join(input_fields)
                sql_function_expr = '{func}({fields})'.format(func=sql_function_name, fields=input_fields)
        if target_field in (NOT_SET, None):
            return sql_function_expr
        else:
            return '{func} AS {target}'.format(func=sql_function_expr, target=target_field)

    @abstractmethod
    def apply_inplace(self, item: Item) -> None:
        pass

    def warning(self, msg: str) -> None:
        logger = self.get_logger()
        if not (isinstance(logger, LoggerInterface) or hasattr(logger, 'warning')):
            logger = FallbackLogger()
        return logger.warning(msg)

    def _get_linked_fields_descriptions(
            self,
            fields: Union[Iterable, Auto] = AUTO,
            group_name: str = 'used',
            prefix: str = '    - ',
            max_len: int = DEFAULT_LINE_LEN,
    ) -> Generator:
        fields = list(Auto.delayed_acquire(fields, self.get_linked_fields))
        count = len(fields)
        yield '{count} {name} fields:'.format(count=count, name=group_name)
        for f in fields:
            if isinstance(f, FieldInterface) or hasattr(f, 'get_one_line_repr'):
                f_repr = f.get_one_line_repr(max_len=max_len)
            else:
                f_repr = repr(f)
            f_repr = prefix + f_repr
            if len(f_repr) > max_len:
                f_repr = f_repr[:max_len - len(CROP_SUFFIX)] + CROP_SUFFIX
            yield f_repr

    def get_detailed_fields_description(self) -> Generator:
        for f in self.get_linked_fields():
            if hasattr(f, 'get_meta_description'):
                # yield repr(f) + ':'
                yield from f.get_meta_description()

    def get_data_description(
            self,
            count: Optional[int] = None,
            title: Optional[str] = 'Detailed fields descriptions',
            max_len: int = DEFAULT_LINE_LEN,
    ) -> Generator:
        input_fields = self.get_input_fields()
        output_fields = self.get_output_fields()
        if input_fields:
            yield from self._get_linked_fields_descriptions(fields=input_fields, group_name='input', max_len=max_len)
        if output_fields:
            yield from self._get_linked_fields_descriptions(fields=output_fields, group_name='output', max_len=max_len)
        detailed_description = list(self.get_detailed_fields_description())
        if detailed_description:
            if count:
                detailed_description = detailed_description[:count]
            yield '\n{title}:\n'.format(title=title)
            yield from detailed_description

    def get_brief_repr(self) -> str:
        inputs = ', '.join(map(get_name, self.get_input_field_names()))
        target = get_name(self.get_target_field_name())
        try:
            func_name = get_name(self.get_function(), or_callable=False)
        except AttributeError as e:
            raise AttributeError('{func}: {e}'.format(func=repr(self.get_function()), e=e))
        if func_name == '<lambda>':
            func_name = 'lambda'
        return f'{target}={func_name}({inputs})'

    def __repr__(self):
        return self.get_brief_repr()


class SingleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            field: Field,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
            default: Any = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        assert isinstance(field, (FieldName, FieldNo, FieldInterface)), 'got {} as {}'.format(field, type(field))
        self._target = field
        self._default = default

    def must_finish(self) -> bool:
        return False

    def get_default_value(self) -> Value:
        return self._default

    def get_target_field(self) -> Field:
        return self._target

    def get_output_fields(self, *args) -> list:
        return [self.get_target_field()]

    def get_input_fields(self) -> list:
        return list()

    def get_target_field_name(self) -> Name:
        return get_name(self.get_target_field())

    def get_input_values(self, item: Item) -> list:
        return it.get_fields_values_from_item(
            fields=self.get_input_field_names(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.must_skip_errors(), logger=self.get_logger(), default=self.get_default_value(),
        )

    def get_output_field_names(self, *args) -> list:
        return [self.get_target_field_name()]

    def get_function(self) -> Callable:
        return lambda i: i

    def get_annotations(self) -> dict:
        function = self.get_function()
        if hasattr(function, '__annotations__'):
            return function.__annotations__
        else:
            return dict()

    def set_target_field(self, field: Field, inplace: bool):
        if inplace:
            self._target = field
            return self
        else:
            return self.make_new(field=field)

    def to(self, field: Field) -> AbstractDescription:
        known_target_field = self.get_target_field()
        if known_target_field in ('_', AUTO, None):
            return self.set_target_field(field, inplace=True) or self
        elif field == known_target_field:
            return self
        else:
            msg = f'[Multi]SelectionDescription not implemented, got {field} = {known_target_field}'
            raise NotImplementedError(msg)

    @abstractmethod
    def get_value_from_item(self, item: Item) -> Value:
        pass

    def apply_inplace(self, item: Item) -> None:
        item_type = self.get_input_item_type()
        if item_type == AUTO:
            item_type = ItemType.detect(item, default=ItemType.Any)
        it.set_to_item_inplace(
            field=self.get_target_field_name(),
            value=self.get_value_from_item(item),
            item=item,
            item_type=item_type,
        )


class MultipleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )


class TrivialMultipleDescription(MultipleFieldDescription, ABC):
    def __init__(
            self,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )

    def must_finish(self) -> bool:
        return False

    @staticmethod
    def is_trivial_multiple() -> bool:
        return True

    def get_function(self) -> Callable:
        return lambda i: i

    def get_input_fields(self, struct: UniKey) -> Iterable:
        return self.get_output_fields(struct)

    def get_output_field_types(self, struct: UniKey) -> list:
        names = self.get_output_field_names(struct)
        types = [struct.get_field_description(f).get_value_type() for f in names]
        return types

    def apply_inplace(self, item: Item) -> None:
        pass
