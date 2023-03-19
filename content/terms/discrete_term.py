from typing import Optional, Callable

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_names
    from content.terms.abstract_term import AbstractTerm, TermType, TermDataAttribute, Field, FieldRoleType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.functions.arguments import get_names
    from .abstract_term import AbstractTerm, TermType, TermDataAttribute, Field, FieldRoleType

Native = AbstractTerm
FieldRole = FieldRoleType  # deprecated


class DiscreteTerm(AbstractTerm):
    def __init__(
            self,
            name: str,
            caption: str = '',
            fields: Optional[dict] = None,
            dicts: Optional[dict] = None,
            mappers: Optional[dict] = None,
            datasets: Optional[dict] = None,
            relations: Optional[dict] = None,
            data: Optional[dict] = None,
    ):
        super().__init__(
            name=name, caption=caption,
            fields=fields, mappers=mappers, datasets=datasets, data=data,
            relations=relations,
        )
        self.add_dicts(dicts)

    def get_term_type(self) -> TermType:
        return TermType.Discrete

    def add_dicts(self, value: Optional[dict] = None, **kwargs) -> Native:
        updated_term = self.add_to_data(TermDataAttribute.Dictionaries, value=value, **kwargs)
        return self._assume_native(updated_term)

    def add_dictionary(self, src: Field, dst: Field, dictionary: dict) -> Native:
        return self.add_dicts({get_names([src, dst]): dictionary})

    def get_dictionary(self, src: Field, dst: Field) -> dict:
        dictionary = self.get_from_data(key=TermDataAttribute.Dictionaries, subkey=(src, dst))
        if dictionary:
            return dictionary
        else:
            return self.get_from_data(key=TermDataAttribute.Dictionaries, subkey=get_names([src, dst]))

    def get_mapper(self, src: Field, dst: Field, default: Optional[Callable] = None) -> Optional[Callable]:
        mapper = super().get_mapper(src, dst)
        if mapper:
            return mapper
        else:
            dictionary = self.get_dictionary(src, dst)
            if dictionary:
                return lambda i: dictionary.get
        return default

    @staticmethod
    def _assume_native(term) -> Native:
        return term
