from typing import Optional

try:  # Assume we're a submodule in a package.
    from content.fields.field_interface import FieldInterface
    from content.fields.field_role_type import FieldRoleType
    from content.terms.continual_term import ContinualTerm, TermType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..fields.field_interface import FieldInterface
    from ..fields.field_role_type import FieldRoleType
    from .continual_term import ContinualTerm, TermType


class ProcessTerm(ContinualTerm):
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
        super().__init__(
            name=name, caption=caption,
            fields=fields, mappers=mappers, datasets=datasets, relations=relations,
            data=data,
        )

    def get_term_type(self) -> TermType:
        return TermType.Process

    def get_value_field(self, **kwargs) -> FieldInterface:
        return self.get_field_by_role(FieldRoleType.Value, **kwargs)

    def get_share_field(self, **kwargs) -> FieldInterface:
        return self.get_field_by_role(FieldRoleType.Share, **kwargs)


TermType.add_classes(process=ProcessTerm)
