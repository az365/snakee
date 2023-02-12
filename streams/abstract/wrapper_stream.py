from abc import ABC

try:  # Assume we're a submodule in a package.
    from interfaces import Stream, Connector, Context, AutoName, Auto
    from streams.abstract.abstract_stream import AbstractStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Stream, Connector, Context, AutoName, Auto
    from .abstract_stream import AbstractStream


class WrapperStream(AbstractStream, ABC):
    def __init__(
            self,
            data,
            name: AutoName = Auto,
            caption: str = '',
            source: Connector = None,
            context: Context = None,
            check: bool = False,
    ):
        super().__init__(
            data=data, check=check,
            name=name, caption=caption,
            source=source, context=context,
        )
