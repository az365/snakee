from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from utils.arguments import get_name, get_names
    from content.fields.field_type import FieldType
    from content.terms.discrete_term import DiscreteTerm, TermType, Field, FieldRole
    from content.terms.object_term import ObjectTerm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...utils.arguments import get_name, get_names
    from ..fields.field_type import FieldType
    from .discrete_term import DiscreteTerm, TermType, Field, FieldRole
    from .object_term import ObjectTerm

Native = DiscreteTerm
Level = Union[int, str, ObjectTerm, Auto]


class HierarchicTerm(DiscreteTerm):
    def __init__(
            self,
            name: str,
            caption: str = '',
            levels: Optional[list] = None,
            fields: Optional[dict] = None,
            dicts: Optional[dict] = None,
            mappers: Optional[dict] = None,
            datasets: Optional[dict] = None,
            data: Optional[dict] = None,
    ):
        self._levels = list()
        super().__init__(
            name=name, caption=caption,
            fields=fields, dicts=dicts, mappers=mappers, datasets=datasets,
            data=data,
        )
        self.set_level_terms(levels)

    def add_level(self, level: Iterable) -> Native:
        assert get_name(level) not in self.get_level_names()
        if isinstance(level, str):
            template = '{name} level {no} ({caption})'
            caption = template.format(name=self.get_name(), no=self.get_count(), caption=self.get_caption())
            level = ObjectTerm(level, caption=caption)
        assert isinstance(level, ObjectTerm)
        self.get_level_terms().append(level)
        return self

    def get_level_terms(self) -> list:
        return self._levels

    def get_level_names(self) -> list:
        return get_names(self.get_level_terms())

    def set_level_terms(self, levels: Iterable) -> Native:
        self._levels = list()
        for level in levels:
            self.add_level(level)
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
                if get_name(cur_level) == get_name(level):
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
        return get_name(self.get_level_term(level))

    def get_level_id_field(self, level: Level = AUTO) -> Field:
        return self.get_level_term(level).get_id_field()

    def get_key_field(self, level: Level = AUTO, default_type: FieldType = FieldType.Str, **kwargs) -> Field:
        if Auto.is_auto(level) or level is None:
            key_field = self.get_field_by_role(FieldRole.Key, default_type=default_type, **kwargs)
        else:
            level_term = self.get_level_term(level)
            key_field = level_term.get_field_by_role(FieldRole.Key, default_type=default_type, **kwargs)
        return key_field

    def get_ids_field(self, level: Level = AUTO, default_type: FieldType = FieldType.Tuple, **kwargs) -> Field:
        if Auto.is_auto(level) or level is None:
            key_field = self.get_field_by_role(FieldRole.Ids, default_type=default_type, **kwargs)
        else:
            level_term = self.get_level_term(level)
            key_field = level_term.get_field_by_role(FieldRole.Ids, default_type=default_type, **kwargs)
        return key_field

    def get_id_fields(self, level: Union[int, str, ObjectTerm, Auto] = AUTO) -> list:
        fields = list()
        for level_depth in range(self.get_depth(level) + 1):
            fields.append(self.get_level_id_field(level_depth))
        return fields

    def get_key_selection_tuple(self, including_target: bool, level: Level = AUTO, delimiter: str = '|'):
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
