from abc import ABC

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from streams.abstract.abstract_stream import AbstractStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .abstract_stream import AbstractStream


class WrapperStream(AbstractStream, ABC):
    def __init__(
            self,
            data,
            name=arg.AUTO,
            source=None,
            context=None,
    ):
        super().__init__(
            data=data,
            name=name,
            source=source,
            context=context,
        )
