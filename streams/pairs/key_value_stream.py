from typing import Union, Callable, Iterable, Optional

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.interfaces.pair_stream_interface import PairStreamInterface
    from streams.stream_type import StreamType
    from streams.simple.row_stream import RowStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..interfaces.regular_stream_interface import RegularStreamInterface
    from ..interfaces.pair_stream_interface import PairStreamInterface
    from ..stream_type import StreamType
    from ..simple.row_stream import RowStream

Native = PairStreamInterface
Stream = RegularStreamInterface


class KeyValueStream(RowStream, PairStreamInterface):
    def __init__(
            self,
            data,
            name=arg.DEFAULT, check=True,
            count=None, less_than=None,
            value_stream_type: Union[StreamType, str] = None,
            source=None, context=None,
            max_items_in_memory=arg.DEFAULT,
            tmp_files_template=arg.DEFAULT,
            tmp_files_encoding=arg.DEFAULT,
    ):
        super().__init__(
            data,
            name=name, check=check,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        if value_stream_type is None:
            self.value_stream_type = StreamType.AnyStream
        else:
            try:
                value_stream_type = StreamType(value_stream_type)
            except ValueError:
                value_stream_type = StreamType(value_stream_type.value)
            self.value_stream_type = value_stream_type or StreamType.AnyStream

    @classmethod
    def is_valid_item_type(cls, item) -> bool:
        if isinstance(item, (list, tuple)):
            return len(item) == 2
        return False

    @staticmethod
    def _get_key(item):
        return item[0]

    @staticmethod
    def _get_value(item):
        return item[1]

    def get_keys(self):
        keys = self.get_mapped_items(self._get_key)
        return list(keys) if self.is_in_memory() else keys

    def get_values(self):
        values = self.get_mapped_items(self._get_value)
        return list(values) if self.is_in_memory() else values

    def map(self, function: Callable, to: Union[StreamType, arg.DefaultArgument] = arg.DEFAULT) -> Native:
        stream = super().map(function, to=to)
        return self._assume_native(stream)

    def map_keys(self, func: Callable) -> Native:
        return self.map(lambda i: (func(i[0]), i[1]))

    def map_values(self, func: Callable) -> Native:
        return self.map(lambda i: (i[0], func(i[1])))

    def get_value_stream_type(self) -> StreamType:
        return self.value_stream_type

    def values(self) -> RegularStreamInterface:
        stream = self.stream(
            self.get_values(),
            stream_type=self.get_value_stream_type(),
        )
        return self._assume_regular(stream)

    def keys(self, uniq, stream_type=arg.DEFAULT) -> RegularStreamInterface:
        stream = self.stream(
            self.get_uniq_keys() if uniq else self.get_keys(),
            stream_type=arg.undefault(stream_type, StreamType.AnyStream),
        )
        return self._assume_regular(stream)

    def get_uniq_keys(self) -> list:
        my_keys = list()
        for i in self.get_items():
            key = self._get_key(i)
            if key in my_keys:
                pass
            else:
                yield key
        return my_keys

    def extract_keys_in_memory(self) -> tuple:
        stream_for_keys, stream_for_items = self.tee_streams(2)
        return (
            stream_for_keys.keys(),
            stream_for_items,
        )

    def extract_keys(self) -> tuple:
        if self.is_in_memory():
            return self.extract_keys_in_memory()
        else:
            if hasattr(self, 'extract_keys_on_disk'):
                return self.extract_keys_on_disk()
            else:
                raise AttributeError('extract_keys_on_disk')

    def sort_by_key(self) -> Native:
        stream = super().sort(self._get_key)
        return self._assume_native(stream)

    def sort(self) -> Native:
        return self.sort_by_key()

    def memory_sort_by_key(self, reverse=False) -> Native:
        stream = self.memory_sort(
            key=self._get_key,
            reverse=reverse,
        )
        return self._assume_native(stream)

    def disk_sort_by_key(self, reverse=False, step=arg.DEFAULT) -> Native:
        step = arg.undefault(step, self.max_items_in_memory)
        stream = self.disk_sort(
            key=self._get_key,
            reverse=reverse,
            step=step,
        )
        return self._assume_native(stream)

    def sorted_group_by_key(self) -> Native:
        def get_groups():
            accumulated = list()
            prev_k = None
            for k, v in self.get_data():
                if (k != prev_k) and accumulated:
                    yield prev_k, accumulated
                    accumulated = list()
                prev_k = k
                accumulated.append(v)
            yield prev_k, accumulated
        sm_groups = self.stream(
            get_groups(),
        )
        if self.is_in_memory():
            sm_groups = sm_groups.to_memory()
        return self._assume_native(sm_groups)

    def group_by_key(self) -> Native:
        stream = self.sort_by_key().sorted_group_by_key()
        return self._assume_native(stream)

    def sorted_group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = True) -> Stream:  # tmp
        assert not keys
        assert not values
        return self.sorted_group_by_key()

    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = True) -> Stream:  # tmp
        assert not keys
        assert not values
        return self.group_by_key()

    def _get_ungrouped(self) -> Iterable:
        for k, a in self.get_items():
            if a:
                for v in a:
                    yield k, v
            else:
                yield k, None

    def ungroup_values(self) -> Native:
        stream = self.stream(
            self._get_ungrouped(),
            ex=('count', 'less_than'),
        )
        return self._assume_native(stream)

    def get_dict(self, of_lists: bool = False) -> dict:
        result = dict()
        if of_lists:
            for k, v in self.get_items():
                distinct = result.get(k, [])
                if v not in distinct:
                    result[k] = distinct + [v]
        else:
            for k, v in self.get_items():
                result[k] = v
        return result

    @staticmethod
    def _assume_regular(stream) -> RegularStreamInterface:
        return stream

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
