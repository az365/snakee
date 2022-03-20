from abc import ABC, abstractmethod
from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.interfaces.data_interface import SimpleDataInterface
    from base.classes.auto import AUTO, Auto
    from content.fields.field_type import FieldType
    from content.fields.field_role_type import FieldRoleType
    from content.fields.field_interface import FieldInterface
    from content.terms.term_type import TermType, TermDataAttribute
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.interfaces.data_interface import SimpleDataInterface
    from ...base.classes.auto import AUTO, Auto
    from ..fields.field_type import FieldType
    from ..fields.field_role_type import FieldRoleType
    from ..fields.field_interface import FieldInterface, FieldType
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
    def get_item(self, key: TermDataAttribute, subkey, skip_missing: Union[bool, Auto] = AUTO, default=None):
        pass

    @abstractmethod
    def add_item(self, key: TermDataAttribute, subkey, value, allow_override: bool = False) -> Native:
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

    @abstractmethod
    def get_field_by_role(
            self,
            role: FieldRoleType,
            default_type: Union[FieldType, Auto, None] = None,
            **kwargs
    ) -> Field:
        pass

    @staticmethod
    @abstractmethod
    def get_default_value_type_by_role(role: FieldRoleType, default_type: FieldType = FieldType.Any) -> FieldType:
        pass
