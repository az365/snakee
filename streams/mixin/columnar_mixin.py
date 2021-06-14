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
    from base.abstract.contextual_data import ContextualDataWrapper
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
    from ...base.abstract.contextual_data import ContextualDataWrapper
    from ...functions import item_functions as fs

Stream = Union[StreamInterface, Any]
OptionalFields = Optional[Union[Iterable, str]]
Key = Union[str, list, tuple]


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
    def get_records(self) -> Iterable:
        pass

    @abstractmethod
    def filter(self, function: Callable) -> Stream:
        pass

    @abstractmethod
    def map(self, function: Callable) -> Stream:
        pass

    @abstractmethod
    def map_side_join(self, right: Stream, key: Key, how='left', right_is_uniq=True) -> Stream:
        pass

    @abstractmethod
    def select(self, *fields, **expressions) -> Stream:
        pass

    @abstractmethod
    def sort(self, *keys, reverse=False) -> Stream:
        pass

    @abstractmethod
    def sorted_group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def is_in_memory(self) -> bool:
        pass

    @abstractmethod
    def get_dataframe(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def show(self, count: int = 10, filters: Optional[list] = None, columns=None):
        pass

    @abstractmethod
    def apply_to_stream(self, function, *args, **kwargs) -> Stream:
        pass


Native = ColumnarInterface


class ColumnarMixin(ContextualDataWrapper, ColumnarInterface, ABC):
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
        stream = self.stream(
            self.get_validated(self.get_items(), skip_errors=skip_errors),
        )
        return self._assume_native(stream)

    def get_shape(self) -> tuple:
        return self.get_count(), self.get_column_count()

    def get_description(self) -> str:
        return '{} rows, {} columns: {}'.format(
            self.get_str_count(),
            self.get_column_count(),
            ', '.join(self.get_columns()),
        )

    def get_column_count(self) -> int:
        return len(list(self.get_columns()))

    def filter(self, function: Callable) -> Stream:
        stream = self.stream(
            filter(function, self.get_items()),
        )
        return self._assume_native(stream)

    def map(self, function: Callable) -> Native:
        stream = self.stream(
            map(function, self.get_items()),
        )
        return self._assume_native(stream)

    def map_side_join(self, right: Native, key: Key, how='left', right_is_uniq=True) -> Native:
        assert how in algo.JOIN_TYPES, 'only {} join types are supported ({} given)'.format(algo.JOIN_TYPES, how)
        keys = arg.update([key])
        joined_items = algo.map_side_join(
            iter_left=self.get_items(),
            iter_right=right.get_items(),
            key_function=fs.composite_key(keys),
            how=how,
            uniq_right=right_is_uniq,
        )
        if self.is_in_memory():
            joined_items = list(joined_items)
        stream = self.stream(joined_items)
        meta = self.get_compatible_static_meta()
        stream = stream.set_meta(**meta)
        return self._assume_native(stream)

    def sorted_group_by(self, *keys, **kwargs) -> Stream:
        stream = self.to_stream()
        return self._assume_native(stream).sorted_group_by(*keys, **kwargs)

    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        return self.sort(*keys).sorted_group_by(*keys, values=values, as_pairs=as_pairs)

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def get_str_count(self) -> str:  # tmp?
        return str(self.get_count())

    def get_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.get_data())

    def get_demo_example(self, count: int = 10, filters: Optional[list] = None, columns: Optional[list] = None):
        sm_sample = self.filter(*filters or []) if filters else self
        sm_sample = sm_sample.take(count)
        if hasattr(sm_sample, 'get_dataframe'):
            return sm_sample.get_dataframe(columns)
        elif hasattr(sm_sample, 'select') and columns:
            return sm_sample.select(*columns).get_items()
        elif hasattr(sm_sample, 'get_items'):
            return sm_sample.get_items()

    def show(self, count: int = 10, filters: Optional[list] = None, columns=None):
        self.log(self.get_description(), truncate=False, force=True)
        return self.get_demo_example(count=count, filters=filters, columns=columns)

    def apply_to_stream(self, function, *args, **kwargs) -> Stream:
        return function(self, *args, **kwargs)
