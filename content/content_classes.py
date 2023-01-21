try:  # Assume we're a submodule in a package.
    from content.fields.field_classes import *
    from content.format.format_classes import *
    from content.items.item_classes import *
    from content.selection.selection_classes import *
    from content.struct.flat_struct import *
    from content.terms.term_classes import *
    from content.documents.document_classes import *
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .fields.field_classes import *
    from .format.format_classes import *
    from .items.item_classes import *
    from .selection.selection_classes import *
    from .struct.flat_struct import *
    from .terms.term_classes import *
    from .documents.document_classes import *
