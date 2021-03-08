from abc import ABC


try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        mappers as ms,
        items as it,
        selection,
        algo,
    )
    from selection import selection_classes as sn
    from loggers import logger_classes as log
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        mappers as ms,
        items as it,
        selection,
        algo,
    )
    from ...selection import selection_classes as sn
    from ...loggers import logger_classes as log
    from ...functions import all_functions as fs


class WrapperStream(sm.AbstractStream, ABC):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            source=None,
            context=None,
    ):
        super().__init__(
            data=data,
            name=name,
            source=source,
            context=context,
        )
