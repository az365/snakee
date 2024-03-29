from abc import ABC, abstractmethod
from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.interfaces.data_interface import SimpleDataInterface
    from content.value_type import ValueType
    from content.fields.field_role_type import FieldRoleType
    from content.fields.field_interface import FieldInterface
    from content.terms.term_type import TermType, TermDataAttribute
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.interfaces.data_interface import SimpleDataInterface
    from ..value_type import ValueType
    from ..fields.field_role_type import FieldRoleType
    from ..fields.field_interface import FieldInterface
    from .term_type import TermType, TermDataAttribute

Native = SimpleDataInterface
Field = Union[FieldInterface, str]


class TermInterface(SimpleDataInterface, ABC):
    @abstractmethod
    def get_term_type(self) -> TermType:
        pass

    @abstractmethod
    def get_type(self) -> TermType:
        pass

    @abstractmethod
    def get_caption(self) -> str:
        pass

    @abstractmethod
    def add_to_data(self, key: Union[TermDataAttribute, str], value: Optional[dict] = None, **kwargs) -> Native:
        pass

    @abstractmethod
    def get_from_data(self, key: Union[TermDataAttribute], subkey=None) -> dict:
        pass

    @abstractmethod
    def add_fields(self, value: Optional[dict] = None, **kwargs) -> Native:
        pass

    @abstractmethod
    def add_mappers(self, value: Optional[dict] = None, **kwargs) -> Native:
        pass

    @abstractmethod
    def add_datasets(self, value: Optional[dict] = None, **kwargs) -> Native:
        pass

    @abstractmethod
    def add_relations(self, value: Optional[dict] = None, update_relations: bool = True, **kwargs) -> Native:
        pass

    @abstractmethod
    def update_relations(self) -> Native:
        pass

    @abstractmethod
    def get_item(self, key: TermDataAttribute, subkey, skip_missing: Optional[bool] = None, default=None):
        pass

    @abstractmethod
    def add_item(self, key: TermDataAttribute, subkey, value, allow_override: bool = False) -> Native:
        pass

    @abstractmethod
    def field(
            self,
            name: str,
            value_type: Optional[ValueType] = None,
            role: Union[FieldRoleType, str, None] = None,
            caption: Optional[str] = None,
            **kwargs
    ) -> FieldInterface:
        pass

    @abstractmethod
    def get_field_by_role(
            self,
            role: FieldRoleType,
            value_type: Optional[ValueType] = None,
            name: Optional[str] = None,
            caption: Optional[str] = None,
            **kwargs
    ) -> Field:
        pass

    @abstractmethod
    def get_fields_by_roles(self) -> dict:
        pass

    @abstractmethod
    def add_mapper(self, src: Field, dst: Field, mapper: Callable) -> Native:
        pass

    @abstractmethod
    def get_mapper(self, src: Field, dst: Field, default: Optional[Callable] = None) -> Optional[Callable]:
        pass
