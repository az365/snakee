from abc import ABC, abstractmethod
from typing import Callable

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from content.fields.field_type import FieldType
    from base.interfaces.data_interface import SimpleDataInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .field_type import FieldType
    from ...base.interfaces.data_interface import SimpleDataInterface


class FieldInterface(SimpleDataInterface, ABC):
    @abstractmethod
    def get_type(self) -> FieldType:
        pass

    @abstractmethod
    def set_type(self, field_type: FieldType, inplace: bool):
        pass

    @abstractmethod
    def get_type_in(self, dialect):
        pass

    @abstractmethod
    def get_type_name(self) -> str:
        pass

    @abstractmethod
    def get_converter(self, source, target) -> Callable:
        pass

    @abstractmethod
    def __add__(self, other):
        pass
