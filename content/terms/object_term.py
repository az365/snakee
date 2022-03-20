from typing import Optional

try:  # Assume we're a submodule in a package.
    from utils.arguments import update
    from content.fields.field_type import FieldType
    from content.terms.discrete_term import DiscreteTerm, TermType, Field, FieldRole
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import update
    from ..fields.field_type import FieldType
    from .discrete_term import DiscreteTerm, TermType, Field, FieldRole

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
            data: Optional[dict] = None,
    ):
        super().__init__(
            name=name, caption=caption,
            fields=fields, dicts=dicts, mappers=mappers, datasets=datasets,
            data=data,
        )

    def get_term_type(self) -> TermType:
        term_type = TermType.Object
        return term_type

    @staticmethod
    def get_default_type_by_role(role: FieldRole, default_type: FieldType = FieldType.Any) -> FieldType:
        field_class = role.get_class()
        if field_class:
            return FieldType.detect_by_type(field_class)
        else:
            return default_type

    def get_id_field(self) -> Field:
        return self.get_field_by_role(FieldRole.Id)

    @staticmethod
    def _assume_native(term) -> Native:
        return term
