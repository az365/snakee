from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        TermInterface, FieldInterface,
        TermType, TermDataAttribute, TermRelation, FieldRoleType, ValueType,
    )
    from base.constants.chars import EMPTY, UNDER, SMALL_INDENT, REPR_DELIMITER, JUPYTER_LINE_LEN
    from base.functions.arguments import get_name, get_names, get_value
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.map_data_mixin import MultiMapDataMixin
    from base.classes.enum import ClassType
    from content.fields.any_field import AnyField
    from content.documents.document_item import Paragraph, Sheet, Chapter
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        TermInterface, FieldInterface,
        TermType, TermDataAttribute, TermRelation, FieldRoleType, ValueType,
    )
    from ...base.constants.chars import EMPTY, UNDER, SMALL_INDENT, REPR_DELIMITER, JUPYTER_LINE_LEN
    from ...base.functions.arguments import get_name, get_names, get_value
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.mixin.map_data_mixin import MultiMapDataMixin
    from ...base.classes.enum import ClassType
    from ..fields.any_field import AnyField
    from ..documents.document_item import Paragraph, Sheet, Chapter

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
            value_type: Optional[ValueType] = None,
            role: Union[FieldRoleType, str, None] = None,
            caption: Optional[str] = None,
            **kwargs
    ) -> FieldInterface:
        if role is None:
            suffix = name.split(UNDER)[-1]
            role = FieldRoleType.detect(suffix, default=FieldRoleType.Undefined)
        return self.get_field_by_role(role, value_type=value_type, name=name, caption=caption, **kwargs)

    def get_fields_by_roles(self) -> dict:
        return self.get_from_data(TermDataAttribute.Fields)

    def get_field_by_role(
            self,
            role: FieldRoleType,
            value_type: Optional[ValueType] = None,
            name: Optional[str] = None,
            caption: Optional[str] = None,
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
            if name is None:
                name = self._get_default_field_name_by_role(role)
            if value_type is None:
                value_type = self._get_default_value_type_by_role(role)
            if caption is None:
                caption = self._get_default_field_caption_by_role(role)
            field = field_class(name, value_type, caption=caption, **kwargs)
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
        if role in FIELD_ROLES_WITHOUT_SUFFIXES or role is None:
            field_name = term_name
        else:
            field_name = FIELD_NAME_TEMPLATE.format(term=term_name, role=get_value(role))
        return field_name

    def _get_default_field_caption_by_role(self, role: FieldRoleType) -> str:
        term_caption = self.get_caption()
        if role in FIELD_ROLES_WITHOUT_SUFFIXES or role is None:
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
        yield self.get_caption()

    def get_fields_sheet(
            self,
            fields_and_roles: Optional[Iterable] = None,
            count: Optional[int] = None,
            name: str = 'Fields sheet',
    ) -> Sheet:
        if not fields_and_roles:
            fields_and_roles = self.get_data().get(TermDataAttribute.Fields)
        columns = 'role', 'name', 'type', 'caption', 'repr'
        records = list()
        for n, (key, value) in enumerate(fields_and_roles.items()):
            if count is not None:
                if n >= count:
                    break
            r = dict(
                role=key,
                name=value.get_name(), caption=value.get_caption(),
                type=value.get_value_type(), repr=value.get_representation(),
            )
            records.append(r)
        return Sheet(records, columns=columns, name=name)

    def get_data_chapter(
            self,
            count: Optional[int] = None,
            title: Optional[str] = 'Data',
            comment: Optional[str] = None,
    ) -> Chapter:
        chapter = Chapter()
        for key in TermDataAttribute.get_enum_items():  # fields, dictionaries, mappers, datasets, relations
            data = self.get_data().get(key)
            if data:
                name = key.get_name()
                title = Paragraph(name, level=3, name=f'{name} title')
                chapter.append(title, inplace=True)
                if key == TermDataAttribute.Fields:
                    sheet = self.get_fields_sheet(data, count=count, name=f'{name} sheet')
                    chapter.append(sheet, inplace=True)
                elif isinstance(data, dict):
                    records = map(lambda i: dict(key=repr(i[0]), value=repr(i[1])), data.items())
                    sheet = Sheet(records, columns=('key', 'value'), name=f'{name} sheet')
                    chapter.append(sheet, inplace=True)
                elif isinstance(data, Iterable) and not isinstance(data, str):
                    records = map(lambda n, i: {'#': n, 'item': repr(i)}, enumerate(data))
                    sheet = Sheet(records, columns=('#', 'item'), name=f'{name} sheet')
                    chapter.append(sheet, inplace=True)
                else:
                    paragraph = Paragraph(data, name=f'{name} paragraph')
                    chapter.append(paragraph, inplace=True)
        return chapter

    # @deprecated_with_alternative('get_data_chapter()')
    def display_data_sheet(
            self,
            count: Optional[int] = None,
            title: Optional[str] = 'Data',
            comment: Optional[str] = None,
            display=None,
    ) -> Native:
        display = self.get_display(display)
        item = self.get_data_chapter()
        display.display_item(item)
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
