from abc import ABC, abstractmethod
from typing import Union, Iterable, Callable, Any, Optional
import pandas as pd

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        numeric as nm,
        algo,
    )
    from items.base_item_type import ItemType
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from base.abstract.data import DataWrapper
    from functions import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        numeric as nm,
        algo,
    )
    from ...items.base_item_type import ItemType
    from ..stream_type import StreamType
    from ..interfaces.abstract_stream_interface import StreamInterface
    from ...base.abstract.data import DataWrapper
    from ...functions import item_functions as fs

Stream = Union[StreamInterface, Any]
OptionalFields = Optional[Union[Iterable, str]]


class ColumnarInterface(StreamInterface, ABC):
    @staticmethod
    @abstractmethod
    def get_item_type() -> ItemType:
        pass

    @abstractmethod
    def get_columns(self) -> list:
        pass

    @abstractmethod
    def get_one_column_values(self, column) -> Iterable:
        pass

    @abstractmethod
    def select(self, *fields, **expressions) -> Stream:
        pass

    @abstractmethod
    def sort(self, *keys, reverse=False) -> Stream:
        pass

    @abstractmethod
    def is_in_memory(self) -> bool:
        pass

    @abstractmethod
    def get_records(self) -> Iterable:
        pass


Native = ColumnarInterface


class ColumnarMixin(DataWrapper, ColumnarInterface, ABC):
    @classmethod
    def is_valid_item(cls, item) -> bool:
        return cls.get_item_type().isinstance(item)

    @classmethod
    def get_validated(cls, items, skip_errors=False, context=None):
        for i in items:
            if cls.is_valid_item(i):
                yield i
            else:
                message = 'get_validated(): item {} is not a {}'.format(i, cls.get_item_type())
                if skip_errors:
                    if context:
                        context.get_logger().log(message)
                else:
                    raise TypeError(message)

    def validated(self, skip_errors=False) -> ColumnarInterface:
        return self.stream(
            self.get_validated(self.get_items(), skip_errors=skip_errors),
        )

    def get_shape(self) -> tuple:
        return self.get_count(), self.get_column_count()

    def get_description(self) -> str:
        return '{} rows, {} columns: {}'.format(
            self.get_str_count(),
            self.get_column_count(),
            ', '.join(self.get_columns()),
        )

    def map(self, function: Callable) -> Native:
        return self.stream(
            map(function, self.get_items()),
        )

    def get_column_count(self) -> int:
        return len(list(self.get_columns()))

    def map_side_join(self, right, key, how='left', right_is_uniq=True) -> Native:
        assert isinstance(right, ColumnarInterface)
        assert how in algo.JOIN_TYPES, 'only {} join types are supported ({} given)'.format(algo.JOIN_TYPES, how)
        keys = arg.update([key])
        joined_items = algo.map_side_join(
            iter_left=self.get_items(),
            iter_right=right.get_items(),
            key_function=fs.composite_key(keys),
            how=how,
            uniq_right=right_is_uniq,
        )
        stream = self.stream(
            list(joined_items) if self.is_in_memory() else joined_items,
        ).set_meta(
            **self.get_static_meta()
        )
        return self._assume_native(stream)

    def sorted_group_by(self, *keys) -> Native:
        return self.stream(
            self.select(keys, lambda r: r).get_items(),
            # to=StreamType.KeyValueStream,
            to='KeyValueStream',
        )

    def group_by(self, *keys) -> Stream:
        return self.sort(*keys).sorted_group_by(*keys)

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def get_str_count(self) -> str:  # tmp?
        return str(self.get_count())

    def get_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.get_data())

    def get_demo_example(self, count=10, filters=[], columns=None):
        sm_sample = self.filter(*filters) if filters else self
        return sm_sample.take(count).get_dataframe(columns)

    def show(self, count=10, filters=[], columns=None):
        self.log(self.get_description(), truncate=False, force=True)
        return self.get_demo_example(count=count, filters=filters, columns=columns)
