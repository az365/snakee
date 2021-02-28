from abc import ABC, abstractmethod


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


class IterableStream(sm.AbstractStream, ABC):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            source=None,
            context=None,
            count=None,
            less_than=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
    ):
        super().__init__(
            data=data,
            name=name,
            source=source,
            context=context,
        )
        if isinstance(data, (list, tuple)):
            self.count = len(data)
        else:
            self.count = count
        self.less_than = less_than or self.count
        self.max_items_in_memory = max_items_in_memory

    def get_items(self):
        return self.get_data()

    def get_meta_except_count(self):
        meta = self.get_meta()
        meta.pop('count')
        meta.pop('less_than')  # ?
        return meta

    def close(self, recursively=False):
        try:
            self.pass_items()
            closed_streams = 1
        except BaseException as e:
            self.log(['Error while trying to close stream:', e], level=log.LoggingLevel.Warning)
            closed_streams = 0
        closed_links = 0
        if recursively:
            for link in self.get_links():
                if hasattr(link, 'close'):
                    closed_links += link.close() or 0
        return closed_streams, closed_links

    def iterable(self):
        yield from self.get_items()

    def next(self):
        return next(
            self.iterable(),
        )

    def one(self):
        for i in self.get_items():
            return i

    def expected_count(self):
        return self.count

    def final_count(self):
        result = 0
        for _ in self.get_items():
            result += 1
        return result

    def get_count(self, in_memory=False, final=False):
        if in_memory:
            self.data = self.get_list()
            self.count = len(self.data)
            return self.count
        elif final:
            return self.final_count()
        else:
            return self.expected_count()

    def estimate_count(self):
        return self.count or self.less_than

    def tee(self, n=2):
        return [
            self.__class__(
                i,
                count=self.count,
            ) for i in tee(
                self.get_items(),
                n,
            )
        ]

    def calc(self, function):
        return function(self.data)

    def lazy_calc(self, function):
        yield from function(self.data)

    def apply_to_data(self, function, to=arg.DEFAULT, save_count=False, lazy=True):
        stream_type = arg.undefault(to, self.get_stream_type())
        target_class = sm.get_class(stream_type)
        if to == arg.DEFAULT:
            props = self.get_meta() if save_count else self.get_meta_except_count()
        else:
            props = dict(count=self.count) if save_count else dict()
        return target_class(
            self.lazy_calc(function) if lazy else self.calc(function),
            **props
        )
