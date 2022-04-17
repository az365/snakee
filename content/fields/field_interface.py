from abc import ABC, abstractmethod
from typing import Type, Callable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoName, AutoBool, Class
    from base.interfaces.data_interface import SimpleDataInterface
    from connectors.databases.dialect_type import DialectType
    from content.representations.repr_interface import RepresentationInterface
    from content.value_type import ValueType
    from content.fields.field_role_type import FieldRoleType
    from content.fields.field_edge_type import FieldEdgeType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import AUTO, Auto, AutoName, AutoBool, Class
    from ...base.interfaces.data_interface import SimpleDataInterface
    from ...connectors.databases.dialect_type import DialectType
    from ..representations.repr_interface import RepresentationInterface
    from ..value_type import ValueType
    from .field_role_type import FieldRoleType
    from .field_edge_type import FieldEdgeType

Native = SimpleDataInterface
Transform = Any
AutoRepr = Union[RepresentationInterface, str, Auto]


class FieldInterface(SimpleDataInterface, ABC):
    @staticmethod
    @abstractmethod
    def get_role() -> FieldRoleType:
        pass

    @abstractmethod
    def set_role_outplace(self, role: FieldRoleType) -> Native:
        pass

    @abstractmethod
    def get_value_type(self) -> ValueType:
        pass

    @abstractmethod
    def get_type_in(self, dialect: DialectType) -> Union[Type, str]:
        pass

    @abstractmethod
    def get_value_type_name(self) -> str:
        pass

    @abstractmethod
    def get_converter(self, source: DialectType, target: DialectType) -> Callable:
        pass

    @abstractmethod
    def set_value_type(self, value_type: ValueType, inplace: bool) -> Native:
        pass

    @abstractmethod
    def get_representation(self) -> Union[RepresentationInterface, str, None]:
        pass

    @abstractmethod
    def set_representation(self, representation: Union[RepresentationInterface, str], inplace: bool) -> Native:
        pass

    @abstractmethod
    def set_repr(self, representation: AutoRepr = AUTO, inplace: bool = False, **kwargs) -> Native:
        pass

    @abstractmethod
    def get_repr_class(self) -> Class:
        pass

    @abstractmethod
    def is_numeric(self) -> bool:
        pass

    @abstractmethod
    def is_boolean(self) -> bool:
        pass

    @abstractmethod
    def is_string(self) -> bool:
        pass

    @classmethod
    @abstractmethod
    def set_struct_builder(cls, struct_builder: Callable) -> None:
        pass

    @classmethod
    @abstractmethod
    def get_struct_builder(cls, default: Callable = list) -> Callable:
        pass

    @abstractmethod
    def get_sql_expression(self) -> str:
        pass

    @abstractmethod
    def get_str_repr(self) -> str:
        pass

    @abstractmethod
    def get_brief_repr(self) -> str:
        pass

    @abstractmethod
    def format(self, value, skip_errors: bool = False) -> str:
        pass

    @abstractmethod
    def is_valid(self) -> AutoBool:
        pass

    @abstractmethod
    def set_valid(self, is_valid: bool, inplace: bool) -> Native:
        pass

    @abstractmethod
    def check_value(self, value) -> bool:
        pass

    @abstractmethod
    def to(self, target: Union[Native, str]) -> Transform:
        pass

    @abstractmethod
    def as_type(self, field_type: ValueType) -> Transform:
        pass

    @abstractmethod
    def drop(self) -> Transform:
        pass

    @abstractmethod
    def get_plural(self, suffix: AutoName = AUTO, caption_prefix: str = 'list of ', **kwargs) -> Native:
        pass

    @abstractmethod
    def get_str_headers(self) -> Generator:
        pass

    @abstractmethod
    def get_count(self) -> int:
        pass

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __add__(self, other):
        pass
