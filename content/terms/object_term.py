from typing import Optional

try:  # Assume we're a submodule in a package.
    from utils.arguments import update
    from content.terms.discrete_term import DiscreteTerm, TermType, Field, FieldRoleType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import update
    from .discrete_term import DiscreteTerm, TermType, Field, FieldRoleType

Native = DiscreteTerm


class ObjectTerm(DiscreteTerm):
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
            fields=fields, dicts=dicts, mappers=mappers, datasets=datasets, relations=relations,
            data=data,
        )

    def get_term_type(self) -> TermType:
        term_type = TermType.Object
        return term_type

    def get_id_field(self, **kwargs) -> Field:
        return self.get_field_by_role(FieldRoleType.Id, **kwargs)

    def get_name_field(self, **kwargs) -> Field:
        return self.get_field_by_role(FieldRoleType.Name, **kwargs)

    def get_repr_field(self, **kwargs) -> Field:
        return self.get_field_by_role(FieldRoleType.Repr, **kwargs)

    def get_count_field(self, **kwargs) -> Field:
        return self.get_field_by_role(FieldRoleType.Count, **kwargs)

    @staticmethod
    def _assume_native(term) -> Native:
        return term


TermType.add_classes(object=ObjectTerm)
