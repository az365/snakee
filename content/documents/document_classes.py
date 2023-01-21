try:  # Assume we're a submodule in a package.
    from content.documents.display_mode import DisplayMode
    from content.documents.document_item import (
        CompositionType,
        DocumentItem, Container,
        Sheet, Chart, MultiChart,
        Text, Link, Paragraph, Chapter, Page,
    )
    from content.documents.document_display import DocumentDisplay
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .display_mode import DisplayMode
    from .document_item import (
        CompositionType,
        DocumentItem, Container,
        Sheet, Chart, MultiChart,
        Text, Link, Paragraph, Chapter, Page,
    )
    from .document_display import DocumentDisplay
