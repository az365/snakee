try:  # Assume we're a submodule in a package.
    from content.terms.term_type import TermType, TermDataAttribute, TermRelation, FieldRole
    from content.terms.abstract_term import AbstractTerm
    from content.terms.continual_term import ContinualTerm
    from content.terms.discrete_term import DiscreteTerm
    from content.terms.object_term import ObjectTerm
    from content.terms.hierarchic_term import HierarchicTerm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .term_type import TermType, TermDataAttribute, TermRelation, FieldRole
    from .abstract_term import AbstractTerm
    from .continual_term import ContinualTerm
    from .discrete_term import DiscreteTerm
    from .object_term import ObjectTerm
    from .hierarchic_term import HierarchicTerm
