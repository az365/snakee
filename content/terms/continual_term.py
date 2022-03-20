from typing import Optional

try:  # Assume we're a submodule in a package.
    from content.terms.abstract_term import AbstractTerm, TermType, TermDataAttribute
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .abstract_term import AbstractTerm, TermType, TermDataAttribute


class ContinualTerm(AbstractTerm):
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
        return TermType.Continual
