from typing import Type, Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from base.constants.chars import EMPTY, TAB_INDENT, IDS_DELIMITER, REPR_DELIMITER
    from base.functions.arguments import get_name, get_names, get_value
    from base.functions.errors import get_type_err_msg
    from base.classes.enum import DynamicEnum, Class
    from content.value_type import ValueType
    from content.terms.discrete_term import DiscreteTerm, TermType, Field, FieldRoleType
    from content.terms.object_term import ObjectTerm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import EMPTY, TAB_INDENT, IDS_DELIMITER, REPR_DELIMITER
    from ...base.functions.arguments import get_name, get_names, get_value
    from ...base.functions.errors import get_type_err_msg
    from ...base.classes.enum import DynamicEnum, Class
    from ..value_type import ValueType
    from .discrete_term import DiscreteTerm, TermType, Field, FieldRoleType
    from .object_term import ObjectTerm

Native = DiscreteTerm
IdValue = Union[int, str]
Level = Union[ObjectTerm, DynamicEnum, IdValue, Type, None]


class HierarchicTerm(DiscreteTerm):
    def __init__(
            self,
            name: str,
            caption: str = EMPTY,
            levels: Union[DynamicEnum, Class, list, None] = None,
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
        assert isinstance(level, ObjectTerm), get_type_err_msg(expected=ObjectTerm, got=level, arg='level')
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
        if level is None:
            return self.set_default_level_depth(self.get_depth(level=None))
        elif isinstance(level, ObjectTerm):
            return self.set_default_level_term(level)
        elif isinstance(level, int):
            level_depth = level
        else:  # isinstance(level, str)
            level_depth = self.get_depth(level)
        return self.set_default_level_depth(level_depth)

    def set_default_level_depth(self, level: int) -> Native:
        max_depth = self.get_depth()
        assert level <= max_depth, f'Expected level <= depth, got {level} > {max_depth}'
        level_term = self.get_level_terms()[level]
        self._default_level = level_term
        return self

    def set_default_level_term(self, level: ObjectTerm) -> Native:
        assert level in self.get_level_terms()
        self._default_level = self
        return self

    def get_count(self) -> int:
        return len(self.get_level_terms())

    def get_depth(self, level: Level = None) -> int:
        if level is None:
            return self.get_count() - 1
        elif isinstance(level, int):
            depth = level
            count = self.get_count()
            if depth >= count:
                msg = f'HierarchicTerm.get_depth(level={level}): expected level < {count}, got {level}'
                raise IndexError(msg)
        else:
            depth = None
            for no, cur_level in enumerate(self.get_level_terms()):
                if get_name(cur_level).lower() == get_name(level).lower():
                    depth = no
            if depth is None:
                raise IndexError(f'HierarchicTerm.get_depth({level}): level not found')
        return depth

    def get_level_term(self, level: Level = None) -> ObjectTerm:
        level_depth = self.get_depth(level)
        level_term = self.get_level_terms()[level_depth]
        assert isinstance(level_term, ObjectTerm), get_type_err_msg(level_term, expected=ObjectTerm, arg='level_term')
        return level_term

    def get_level_name(self, level: Level = None) -> str:
        return self.get_level_term(level).get_name()

    def get_level_id_field(self, level: Level = None) -> Field:
        return self.get_level_term(level).get_id_field()

    def get_id_field(self, level: Level = None, **kwargs) -> Field:
        if level is None:
            level = self.get_default_level_term()
        assert isinstance(level, ObjectTerm), get_type_err_msg(expected=ObjectTerm, got=level, arg='level')
        if self._ids_is_independent or get_name(level) == self.get_level_name(0):
            if 'caption' not in kwargs:
                if not level.get_caption():
                    kwargs['caption'] = f'id of {get_name(level)} ({self.get_caption()})'
            return level.get_id_field(**kwargs)
        else:
            return self.get_key_field(level, **kwargs)

    def get_id_value(self, *ids, level: Level = None, delimiter: str = IDS_DELIMITER) -> IdValue:
        if level is None:
            level = self.get_default_level_term()
        assert isinstance(level, ObjectTerm), get_type_err_msg(expected=ObjectTerm, got=level, arg='level')
        if self._ids_is_independent or get_name(level) == self.get_level_name(0):
            depth = self.get_depth(level)
            assert depth < len(ids), f'Expected depth < ids count, got {depth}, {ids}'
            return ids[depth]
        else:
            return self.get_key_value(*ids, level=level, delimiter=delimiter)

    def get_name_field(self, level: Level = None, **kwargs) -> Field:
        if level is None:
            level = self.get_default_level_term()
        assert isinstance(level, ObjectTerm), get_type_err_msg(expected=ObjectTerm, got=level, arg='level')
        return level.get_name_field(**kwargs)

    def get_repr_field(self, level: Level = None, **kwargs) -> Field:
        if level is None:
            level = self.get_default_level_term()
        assert isinstance(level, ObjectTerm), get_type_err_msg(expected=ObjectTerm, got=level, arg='level')
        return level.get_repr_field(**kwargs)

    def get_key_value(self, *ids, level: Level = None, delimiter: str = IDS_DELIMITER) -> IdValue:
        expected_depth = self.get_depth(level)
        got_depth = len(ids) - 1
        assert got_depth >= expected_depth, f'Expected level {expected_depth}, got {got_depth}'
        return delimiter.join(map(str, ids[:expected_depth + 1]))

    def get_key_field(self, level: Level = None, value_type: ValueType = ValueType.Str, **kwargs) -> Field:
        if level is None:
            key_field = self.get_field_by_role(FieldRoleType.Key, value_type=value_type, **kwargs)
        else:
            level_term = self.get_level_term(level)
            key_field = level_term.get_field_by_role(FieldRoleType.Key, value_type=value_type, **kwargs)
        return key_field

    def get_ids_field(self, level: Level = None, default_type: ValueType = ValueType.Sequence, **kwargs) -> Field:
        if level is None:
            key_field = self.get_field_by_role(FieldRoleType.Ids, value_type=default_type, **kwargs)
        else:
            level_term = self.get_level_term(level)
            key_field = level_term.get_field_by_role(FieldRoleType.Ids, value_type=default_type, **kwargs)
        return key_field

    def get_id_fields(self, level: Level = None) -> list:
        fields = list()
        for level_depth in range(self.get_depth(level) + 1):
            fields.append(self.get_level_id_field(level_depth))
        return fields

    def get_key_selection_tuple(
            self,
            including_target: bool,
            level: Level = None,
            delimiter: str = IDS_DELIMITER,
    ) -> tuple:
        selector = [lambda *a: delimiter.join(a)]
        selector += self.get_id_fields(level)
        if including_target:
            selector = [self.get_key_field(level, value_type=ValueType.Str), *selector]
        return tuple(selector)

    def get_ids_selection_tuple(self, including_target: bool, level: Level = None):
        selector = [lambda *a: a]
        selector += self.get_id_fields(level)
        if including_target:
            selector = [self.get_key_field(level, value_type=ValueType.Sequence), *selector]
        return tuple(selector)


TermType.add_classes(hierarchic=HierarchicTerm)
