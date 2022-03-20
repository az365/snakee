from abc import ABC, abstractmethod
from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.abstract.simple_data import SimpleDataWrapper
    from base.classes.auto import AUTO, Auto
    from base.functions.arguments import get_value
    from base.mixin.describe_mixin import DescribeMixin
    from utils.arguments import update, get_names, get_str_from_args_kwargs
    from content.fields.field_interface import FieldInterface, FieldType
    from content.fields.advanced_field import AdvancedField
    from content.terms.term_type import TermType, TermDataAttribute, FieldRole, TermRelation
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.classes.auto import AUTO, Auto
    from ...base.functions.arguments import get_value
    from ...base.mixin.describe_mixin import DescribeMixin
    from ...utils.arguments import update, get_names, get_str_from_args_kwargs
    from ..fields.field_interface import FieldInterface, FieldType
    from ..fields.advanced_field import AdvancedField
    from .term_type import TermType, TermDataAttribute, FieldRole, TermRelation

Native = SimpleDataWrapper
Field = Union[FieldInterface, str]


class AbstractTerm(SimpleDataWrapper, DescribeMixin, ABC):
    def __init__(
            self,
            name: str,
            caption: str = '',
            fields: Optional[dict] = None,
            mappers: Optional[dict] = None,
            datasets: Optional[dict] = None,
            relations: Optional[dict] = None,
            data: Optional[dict] = None,
    ):
        assert name, 'AbstractTerm: name must be non-empty'
        # data = Auto.delayed_acquire(data, dict)
        self._caption = caption
        super().__init__(name=name, data=data or dict())
        self.add_fields(fields)
        self.add_mappers(mappers)
        self.add_datasets(datasets)
        self.add_relations(relations)

    @abstractmethod
    def get_term_type(self) -> TermType:
        pass

    def get_type(self) -> TermType:
        return self.get_term_type()

    def get_caption(self) -> str:
        return self._caption

    def add_to_data(self, key: Union[TermDataAttribute, str], value: Optional[dict] = None, **kwargs) -> Native:
        if not (value or kwargs):
            return self
        if not isinstance(key, TermDataAttribute):
            key = TermDataAttribute(key)
        data = self.get_data()
        assert isinstance(data, dict), 'AbstractTerm.add_to_data(): Expected data as dict, got {}'.format(data)
        if key not in data:
            data[key] = dict()
        data_dict = data[key]
        assert isinstance(data_dict, dict), 'AbstractTerm.add_to_data(): Expected data as dict, got {}'.format(data)
        if value:
            data_dict.update(value)
        data_dict.update(kwargs)
        return self

    def get_from_data(self, key: Union[TermDataAttribute], subkey=None) -> dict:
        data = self.get_data()
        assert isinstance(data, dict), 'AbstractTerm.get_from_data(): Expected data as dict, got {}'.format(data)
        if key not in data:
            data[key] = dict()
        if subkey is None:
            return data[key]
        else:
            return data[key].get(subkey)

    def add_fields(self, value: Optional[dict] = None, **kwargs) -> Native:
        return self.add_to_data(TermDataAttribute.Fields, value=value, **kwargs)

    def add_mappers(self, value: Optional[dict] = None, **kwargs) -> Native:
        return self.add_to_data(TermDataAttribute.Mappers, value=value, **kwargs)

    def add_datasets(self, value: Optional[dict] = None, **kwargs) -> Native:
        return self.add_to_data(TermDataAttribute.Datasets, value=value, **kwargs)

    def add_relations(self, value: Optional[dict] = None, **kwargs) -> Native:
        return self.add_to_data(TermDataAttribute.Relations, value=value, **kwargs)

    def get_item(self, key: TermDataAttribute, subkey, skip_missing: Union[bool, Auto] = AUTO, default=None):
        skip_missing = Auto.acquire(skip_missing, default is not None)
        data_dict = self.get_from_data(key)
        if subkey in data_dict:
            return data_dict[subkey]
        elif skip_missing:
            return default
        else:
            formatter = '{cls}.get_item({key}, {subkey}): item {subkey} not exists: {existing}'
            msg = formatter.format(cls=self.__class__.__name__, key=key, subkey=subkey)
            raise IndexError(msg)

    def add_item(self, key: TermDataAttribute, subkey, value, allow_override: bool = False) -> Native:
        data_dict = self.get_from_data(key)
        if not allow_override:
            if subkey in data_dict:
                existing = data_dict[subkey]
                formatter = '{cls}.add_item({key}, {subkey}, {value}): item {subkey} already exists: {existing}'
                cls = self.__class__.__name__
                msg = formatter.format(cls=cls, key=key, subkey=subkey, value=value, existing=existing)
                raise ValueError(msg)
        data_dict[subkey] = value
        return self

    def get_fields_by_roles(self) -> dict:
        return self.get_from_data(TermDataAttribute.Fields)

    def add_mapper(self, src: Field, dst: Field, mapper: Callable) -> Native:
        key = TermDataAttribute.Mappers
        subkey = get_names([src, dst])
        assert isinstance(key, TermDataAttribute)
        return self.add_item(key, subkey, mapper)

    def get_mapper(self, src: Field, dst: Field, default: Optional[Callable] = None) -> Optional[Callable]:
        key = TermDataAttribute.Mappers
        subkey = get_names([src, dst])
        assert isinstance(key, TermDataAttribute)
        return self.get_item(key, subkey, skip_missing=True, default=default)

    def get_field_by_role(self, role: FieldRole, default_type: Union[FieldType, Auto, None] = None) -> Field:
        default_type = Auto.acquire(default_type, None)
        fields_by_roles = self.get_fields_by_roles()
        role_value = get_value(role)
        if role_value in fields_by_roles:
            return fields_by_roles[role_value]
        else:
            term_name = self.get_name()
            term_caption = self.get_caption()
            field_type = default_type or self.get_default_type_by_role(role)
            if role == FieldRole.Repr or role is None:
                field_name = term_name
                field_caption = term_caption
            else:
                field_name = '{term}_{role}'.format(term=self.get_name(), role=role_value)
                field_caption_template = '{role} of {term} ({caption})'
                field_caption = field_caption_template.format(role=role_value, term=term_name, caption=term_caption)
            field_class = AdvancedField
            field = field_class(field_name, field_type, caption=field_caption)
            fields_by_roles[role_value] = field
            return field

    @staticmethod
    def get_default_type_by_role(role: FieldRole, default_type: FieldType = FieldType.Any) -> FieldType:
        return FieldType.Any

    def __repr__(self):
        return self.get_name()
