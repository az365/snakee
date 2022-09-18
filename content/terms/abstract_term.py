from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        TermInterface, FieldInterface,
        TermType, TermDataAttribute, TermRelation, FieldRoleType, ValueType,
        AUTO, Auto, AutoCount,
    )
    from base.functions.arguments import get_name, get_names, get_value
    from base.constants.chars import EMPTY, UNDER, SMALL_INDENT, REPR_DELIMITER, JUPYTER_LINE_LEN
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.map_data_mixin import MultiMapDataMixin
    from base.classes.enum import ClassType
    from content.fields.any_field import AnyField
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        TermInterface, FieldInterface,
        TermType, TermDataAttribute, TermRelation, FieldRoleType, ValueType,
        AUTO, Auto, AutoCount,
    )
    from ...base.functions.arguments import get_name, get_names, get_value
    from ...base.constants.chars import EMPTY, UNDER, SMALL_INDENT, REPR_DELIMITER, JUPYTER_LINE_LEN
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.mixin.map_data_mixin import MultiMapDataMixin
    from ...base.classes.enum import ClassType
    from ..fields.any_field import AnyField

Native = SimpleDataWrapper
Field = Union[FieldInterface, str]

FIELD_ROLES_WITHOUT_SUFFIXES = FieldRoleType.Repr, FieldRoleType.Value, FieldRoleType.Undefined
FIELD_NAME_TEMPLATE = '{term}' + UNDER + '{role}'
FIELD_CAPTION_TEMPLATE = '{role} of {term} ({caption})'
DESCRIPTION_COLUMN_LENS = 3, 10, 20, 85  # prefix, key, value, caption


class AbstractTerm(SimpleDataWrapper, MultiMapDataMixin, TermInterface, ABC):
    def __init__(
            self,
            name: str,
            caption: str = EMPTY,
            fields: Optional[dict] = None,  # Dict[FieldRoleType, AdvancedField]
            mappers: Optional[dict] = None,  # Dict[MapperName, Description]
            datasets: Optional[dict] = None,
            relations: Optional[dict] = None,
            data: Optional[dict] = None,
    ):
        assert name, 'AbstractTerm: name must be non-empty'
        super().__init__(name=name, caption=caption, data=data or dict())
        self.add_fields(fields)
        self.add_mappers(mappers)
        self.add_datasets(datasets)
        self.add_relations(relations)

    @abstractmethod
    def get_term_type(self) -> TermType:
        pass

    def get_type(self) -> TermType:
        return self.get_term_type()

    @staticmethod
    def get_first_level_key_classes() -> tuple:
        return TermDataAttribute,

    @staticmethod
    def get_first_level_key_default_order() -> tuple:
        return (
            TermDataAttribute.Fields, TermDataAttribute.Datasets, TermDataAttribute.Mappers,
            TermDataAttribute.Relations,
        )

    def add_field(self, field: AnyField, role: FieldRoleType) -> Native:
        return self.add_fields({role: field})

    def add_fields(self, value: Optional[dict] = None, **kwargs) -> Native:
        term = self.add_to_data(TermDataAttribute.Fields, value=value, **kwargs)
        return self._assume_native(term)

    def add_mappers(self, value: Optional[dict] = None, **kwargs) -> Native:
        term = self.add_to_data(TermDataAttribute.Mappers, value=value, **kwargs)
        return self._assume_native(term)

    def add_datasets(self, value: Optional[dict] = None, **kwargs) -> Native:
        term = self.add_to_data(TermDataAttribute.Datasets, value=value, **kwargs)
        return self._assume_native(term)

    def add_relations(self, value: Optional[dict] = None, update_relations: bool = True, **kwargs) -> Native:
        self.add_to_data(TermDataAttribute.Relations, value=value, **kwargs)
        return self.update_relations() if update_relations else self

    def update_relations(self) -> Native:
        relations = self.get_from_data(TermDataAttribute.Relations)
        msg = 'update_relations(): expected {e}, got {a}'
        for k, v in relations.items():
            assert isinstance(k, AbstractTerm) or hasattr(k, 'add_relations'), msg.format(e='AbstractTerm', a=k)
            assert isinstance(v, TermRelation) or hasattr(v, 'get_reversed'), msg.format(e='TermRelation', a=v)
            reversed_relation = v.get_reversed()
            k.add_relations({self: reversed_relation}, update_relations=False)
        return self

    def field(
            self,
            name: str,
            value_type: Union[ValueType, Auto] = AUTO,
            role: Union[FieldRoleType, str, Auto] = AUTO,
            caption: Union[str, Auto] = AUTO,
            **kwargs
    ) -> FieldInterface:
        if Auto.is_auto(role):
            suffix = name.split(UNDER)[-1]
            role = FieldRoleType.detect(suffix, default=FieldRoleType.Undefined)
        return self.get_field_by_role(role, value_type=value_type, name=name, caption=caption, **kwargs)

    def get_fields_by_roles(self) -> dict:
        return self.get_from_data(TermDataAttribute.Fields)

    def get_field_by_role(
            self,
            role: FieldRoleType,
            value_type: Union[ValueType, Auto] = AUTO,
            name: Union[str, Auto] = AUTO,
            caption: Union[str, Auto] = AUTO,
            **kwargs
    ) -> Field:
        fields_by_roles = self.get_fields_by_roles()
        role_value = get_value(role)
        if role_value in fields_by_roles:
            field = fields_by_roles[role_value]
            if kwargs:
                assert isinstance(field, AnyField)
                field = field.set_outplace(**kwargs)
        else:
            field_class = self._get_default_field_class_by_role(role)
            field_name = Auto.delayed_acquire(name, self._get_default_field_name_by_role, role)
            value_type = Auto.delayed_acquire(value_type, self._get_default_value_type_by_role, role)
            field_caption = Auto.delayed_acquire(caption, self._get_default_field_caption_by_role, role)
            field = field_class(field_name, value_type, caption=field_caption, **kwargs)
            fields_by_roles[role_value] = field
        return field

    @staticmethod
    def _get_default_field_class():
        return AnyField

    def _get_default_field_class_by_role(self, role: FieldRoleType):
        default = self._get_default_field_class()
        if not isinstance(role, FieldRoleType):
            role = FieldRoleType(role)
        custom = role.get_class(default=default)
        if isinstance(custom, FieldInterface):
            return custom
        else:
            return default

    def _get_default_field_name_by_role(self, role: FieldRoleType) -> str:
        term_name = self.get_name()
        if role in FIELD_ROLES_WITHOUT_SUFFIXES or role in (None, AUTO):
            field_name = term_name
        else:
            field_name = FIELD_NAME_TEMPLATE.format(term=term_name, role=get_value(role))
        return field_name

    def _get_default_field_caption_by_role(self, role: FieldRoleType) -> str:
        term_caption = self.get_caption()
        if role in FIELD_ROLES_WITHOUT_SUFFIXES or role in (None, AUTO):
            field_caption = term_caption
        else:
            term_name = self.get_name()
            role_value = get_value(role)
            field_caption = FIELD_CAPTION_TEMPLATE.format(role=role_value, term=term_name, caption=term_caption)
        return field_caption

    @staticmethod
    def _get_default_value_type_by_role(role: FieldRoleType, default_type: ValueType = ValueType.Any) -> ValueType:
        if not isinstance(role, FieldRoleType):
            role = FieldRoleType.detect(role)
        return role.get_default_value_type(default=default_type)

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

    def get_str_headers(self) -> Generator:
        yield self.get_brief_repr()
        yield self.get_caption()

    def display_data_sheet(
            self,
            count: Optional[int] = None,
            title: Optional[str] = 'Data',
            comment: Optional[str] = None,
            max_len: AutoCount = AUTO,
            display=AUTO,
    ) -> Native:
        display = self.get_display(display)
        for key in TermDataAttribute.get_enum_items():  # fields, dictionaries, mappers, datasets, relations
            data = self.get_data().get(key)
            if data:
                display.display_paragraph(key.get_name(), level=3)
                if key == TermDataAttribute.Fields:
                    columns = 'role', 'name', 'type', 'caption', 'repr'
                    records = map(
                        lambda i:
                        dict(
                            role=i[0], name=i[1].get_name(), caption=i[1].get_caption(),
                            type=i[1].get_value_type(), repr=i[1].get_representation(),
                        ),
                        data.items(),
                    )
                    display.display_sheet(records, columns=columns, count=count)
                elif isinstance(data, dict):
                    records = map(lambda i: dict(key=i[0], value=i[1]), data.items())
                    display.display_sheet(records, columns=('key', 'value'))
                elif isinstance(data, Iterable) and not isinstance(data, str):
                    records = map(lambda n, i: {'#': n, 'item': i}, enumerate(data))
                    display.display_sheet(records, columns=('#', 'item'))
                else:
                    display.display_paragraph(data)
        return self

    # @deprecated
    def get_type_in(self, dialect=None):  # TMP for compatibility with AbstractField/StructInterface
        return list

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj

    def __hash__(self):
        return hash(self.get_name())

    def __repr__(self):
        return self.get_name()
