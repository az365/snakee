from typing import Type, Optional, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.functions.arguments import get_name, get_names, get_value
    from base.classes.enum import DynamicEnum
    from content.fields.field_type import FieldType
    from content.terms.discrete_term import DiscreteTerm, TermType, Field, FieldRoleType
    from content.terms.object_term import ObjectTerm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...base.functions.arguments import get_name, get_names, get_value
    from ...base.classes.enum import DynamicEnum
    from ..fields.field_type import FieldType
    from .discrete_term import DiscreteTerm, TermType, Field, FieldRoleType
    from .object_term import ObjectTerm

Native = DiscreteTerm
IdValue = Union[int, str]
Level = Union[ObjectTerm, DynamicEnum, IdValue, Type, Auto]

IDS_DELIMITER = '|'


class HierarchicTerm(DiscreteTerm):
    def __init__(
            self,
            name: str,
            caption: str = '',
            levels: Union[DynamicEnum, list, None] = None,
            fields: Optional[dict] = None,
            dicts: Optional[dict] = None,
            mappers: Optional[dict] = None,
            datasets: Optional[dict] = None,
            relations: Optional[dict] = None,
            default_level: Optional[Level] = None,
            ids_is_independent: bool = False,
            data: Optional[dict] = None,
    ):
        self._levels = list()
        self._default_level = None
        self._ids_is_independent = ids_is_independent
        super().__init__(
            name=name, caption=caption,
            fields=fields, dicts=dicts, mappers=mappers, datasets=datasets, relations=relations,
            data=data,
        )
        self.set_level_terms(levels)
        self.set_default_level(default_level)

    def add_level(self, level: ObjectTerm) -> Native:
        assert get_name(level) not in self.get_level_names(), 'Repeated levels not allowed'
        if not isinstance(level, ObjectTerm):
            level = get_value(level)
        if isinstance(level, str):
            template = '{name} level {no} ({caption})'
            caption = template.format(name=self.get_name(), no=self.get_count(), caption=self.get_caption())
            level = ObjectTerm(level, caption=caption)
        assert isinstance(level, ObjectTerm), 'add_level(level): Expected level as ObjectTerm, got {}'.format(level)
        self.get_level_terms().append(level)
        return self

    def get_level_terms(self) -> list:
        return self._levels

    def get_level_names(self) -> list:
        return get_names(self.get_level_terms())

    def set_level_terms(self, levels: Iterable) -> Native:
        self._levels = list()
        if isinstance(levels, DynamicEnum) or hasattr(levels, 'get_enum_items'):
            levels = levels.get_enum_items()
        for level in levels:
            self.add_level(level)
        return self

    def get_default_level_term(self) -> ObjectTerm:
        return self._default_level

    def set_default_level(self, level: Optional[Level]) -> Native:
        if not Auto.is_defined(level):
            return self.set_default_level_depth(self.get_depth(level=AUTO))
        elif isinstance(level, ObjectTerm):
            return self.set_default_level_term(level)
        elif isinstance(level, int):
            level_depth = level
        else:  # isinstance(level, str)
            level_depth = self.get_depth(level)
        return self.set_default_level_depth(level_depth)

    def set_default_level_depth(self, level: int) -> Native:
        max_depth = self.get_depth()
        assert level <= max_depth, 'Expected level <= depth, got {level} > {depth}'.format(level=level, depth=max_depth)
        level_term = self.get_level_terms()[level]
        self._default_level = level_term
        return self

    def set_default_level_term(self, level: ObjectTerm) -> Native:
        assert level in self.get_level_terms()
        self._default_level = self
        return self

    def get_count(self) -> int:
        return len(self.get_level_terms())

    def get_depth(self, level: Level = AUTO) -> int:
        if Auto.is_auto(level) or level is None:
            return self.get_count() - 1
        elif isinstance(level, int):
            depth = level
            count = self.get_count()
            if depth >= count:
                template = 'HierarchicTerm.get_depth(level={level}): expected level < {count}, got {level}'
                raise IndexError(template.format(level=level, count=count))
        else:
            depth = None
            for no, cur_level in enumerate(self.get_level_terms()):
                if get_name(cur_level).lower() == get_name(level).lower():
                    depth = no
            if depth is None:
                raise IndexError('HierarchicTerm.get_depth({level}): level not found'.format(level=level))
        return depth

    def get_level_term(self, level: Level = AUTO) -> ObjectTerm:
        level_depth = self.get_depth(level)
        level_term = self.get_level_terms()[level_depth]
        assert isinstance(level_term, ObjectTerm), 'get_level_term(): Expected ObjectTerm, got {}'.format(level_term)
        return level_term

    def get_level_name(self, level: Level = AUTO) -> str:
        return self.get_level_term(level).get_name()

    def get_level_id_field(self, level: Level = AUTO) -> Field:
        return self.get_level_term(level).get_id_field()

    def get_id_field(self, level: Level = AUTO, **kwargs) -> Field:
        level = Auto.acquire(level, self.get_default_level_term())
        assert isinstance(level, ObjectTerm), 'Expected level as ObjectTerm, got {level}'.format(level=level)
        if self._ids_is_independent or get_name(level) == self.get_level_name(0):
            if 'caption' not in kwargs:
                if not level.get_caption():
                    kwargs['caption'] = 'id of {level} ({term})'.format(level=get_name(level), term=self.get_caption())
            return level.get_id_field(**kwargs)
        else:
            return self.get_key_field(level, **kwargs)

    def get_id_value(self, *ids, level: Level = AUTO, delimiter: str = IDS_DELIMITER) -> IdValue:
        level = Auto.acquire(level, self.get_default_level_term())
        assert isinstance(level, ObjectTerm), 'Expected level as ObjectTerm, got {level}'.format(level=level)
        if self._ids_is_independent or get_name(level) == self.get_level_name(0):
            depth = self.get_depth(level)
            assert depth < len(ids), 'Expected depth < ids, gt {d}, {ids}'.format(d=depth, ids=ids)
            return ids[depth]
        else:
            return self.get_key_value(*ids, level=level, delimiter=delimiter)

    def get_name_field(self, level: Level = AUTO, **kwargs) -> Field:
        level = Auto.acquire(level, self.get_default_level_term())
        assert isinstance(level, ObjectTerm), 'Expected level as ObjectTerm, got {level}'.format(level=level)
        return level.get_name_field(**kwargs)

    def get_repr_field(self, level: Level = AUTO, **kwargs) -> Field:
        level = Auto.acquire(level, self.get_default_level_term())
        assert isinstance(level, ObjectTerm), 'Expected level as ObjectTerm, got {level}'.format(level=level)
        return level.get_repr_field(**kwargs)

    def get_key_value(self, *ids, level: Level = AUTO, delimiter: str = IDS_DELIMITER) -> IdValue:
        expected_depth = self.get_depth(level)
        got_depth = len(ids) - 1
        assert got_depth >= expected_depth, 'Expected level {e}, got {g}'.format(e=expected_depth, g=got_depth)
        return delimiter.join(map(str, ids[:expected_depth + 1]))

    def get_key_field(self, level: Level = AUTO, default_type: FieldType = FieldType.Str, **kwargs) -> Field:
        if Auto.is_auto(level) or level is None:
            key_field = self.get_field_by_role(FieldRoleType.Key, default_type=default_type, **kwargs)
        else:
            level_term = self.get_level_term(level)
            key_field = level_term.get_field_by_role(FieldRoleType.Key, default_type=default_type, **kwargs)
        return key_field

    def get_ids_field(self, level: Level = AUTO, default_type: FieldType = FieldType.Tuple, **kwargs) -> Field:
        if Auto.is_auto(level) or level is None:
            key_field = self.get_field_by_role(FieldRoleType.Ids, default_type=default_type, **kwargs)
        else:
            level_term = self.get_level_term(level)
            key_field = level_term.get_field_by_role(FieldRoleType.Ids, default_type=default_type, **kwargs)
        return key_field

    def get_id_fields(self, level: Level = AUTO) -> list:
        fields = list()
        for level_depth in range(self.get_depth(level) + 1):
            fields.append(self.get_level_id_field(level_depth))
        return fields

    def get_key_selection_tuple(self, including_target: bool, level: Level = AUTO, delimiter: str = '|') -> tuple:
        selector = [lambda *a: delimiter.join(a)]
        selector += self.get_id_fields(level)
        if including_target:
            selector = [self.get_key_field(level, default_type=FieldType.Str), *selector]
        return tuple(selector)

    def get_ids_selection_tuple(self, including_target: bool, level: Level = AUTO):
        selector = [lambda *a: a]
        selector += self.get_id_fields(level)
        if including_target:
            selector = [self.get_key_field(level, default_type=FieldType.Tuple), *selector]
        return tuple(selector)

    def get_meta_description(
            self,
            with_title: bool = True,
            with_summary: bool = True,
            prefix: str = ' ' * 4,
            delimiter: str = ' ',
    ) -> Generator:
        for level_no, level_term in enumerate(self.get_level_terms()):
            yield '\n#{no}: {term}'.format(no=level_no, term=level_term.get_brief_repr())
            if isinstance(level_term, DiscreteTerm):
                yield from level_term.get_data_description()
            else:
                yield str(level_term)


TermType.add_classes(hierarchic=HierarchicTerm)
