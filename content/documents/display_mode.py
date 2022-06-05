try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
    from utils.external import HTML, Markdown
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import ClassType
    from ...utils.external import HTML, Markdown


class DisplayMode(ClassType):
    Text = 'text'
    Md = 'md'
    Html = 'html'

    _dict_classes = dict(
        text=str,
        md=Markdown,
        html=HTML,
    )


DisplayMode.prepare()
