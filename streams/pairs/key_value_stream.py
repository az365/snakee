from typing import Union, Callable, Iterable, Optional

try:  # Assume we're a submodule in a package.
    from interfaces import (
        RegularStreamInterface, PairStreamInterface, StreamType, Struct,
        AUTO, Auto, AutoName, AutoCount,
    )
    from utils.decorators import deprecated, deprecated_with_alternative
    from content.struct.flat_struct import FlatStruct
    from functions.secondary import array_functions as fs
    from streams.regular.row_stream import RowStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        RegularStreamInterface, PairStreamInterface, StreamType, Struct,
        AUTO, Auto, AutoName, AutoCount,
    )
    from ...functions.secondary import array_functions as fs
    from ...content.struct.flat_struct import FlatStruct
    from ...utils.decorators import deprecated, deprecated_with_alternative
    from ..regular.row_stream import RowStream

Native = PairStreamInterface
Stream = RegularStreamInterface

KEY = fs.first()
VALUE = fs.second()


class KeyValueStream(RowStream, PairStreamInterface):
    def __init__(
            self,
            data,
            name: AutoName = AUTO,
            caption: str = '',
            count=None,
            less_than=None,
            struct: Struct = None,
            value_stream_type: Union[StreamType, str] = None,
            source=None, context=None,
            max_items_in_memory: AutoCount = AUTO,
            tmp_files=AUTO,
            check=True,
    ):
        super().__init__(
            data=data, struct=struct, check=check,
            name=name, caption=caption,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )
        self.value_stream_type = None
        self.set_value_stream_type(value_stream_type)

    @deprecated_with_alternative('Struct')
    def get_value_stream_type(self) -> StreamType:
        return self.value_stream_type

    def set_value_stream_type(self, value_stream_type: StreamType) -> Native:
        if not Auto.is_defined(value_stream_type):
            self.value_stream_type = StreamType.AnyStream
        else:
            try:
                value_stream_type = StreamType(value_stream_type)
            except ValueError:
                value_stream_type = StreamType(value_stream_type.value)
            self.value_stream_type = value_stream_type or StreamType.AnyStream
        return self

    def get_struct(self) -> FlatStruct:
        struct = FlatStruct(['key', 'value']).set_types(key='any', value='any')
        assert isinstance(struct, FlatStruct)
        return struct

    @deprecated_with_alternative('select()')
    def map_keys(self, func: Callable) -> Native:
        stream = self.map(lambda i: (func(KEY(i)), VALUE(i)))
        return self._assume_native(stream)

    @deprecated_with_alternative('select()')
    def map_values(self, func: Callable) -> Native:
        stream = self.map(lambda i: (i[0], func(i[1])))
        return self._assume_native(stream)

    @deprecated_with_alternative('get_one_column_values()')
    def values(self) -> RegularStreamInterface:
        stream_type = self.get_value_stream_type()
        stream = self.map_to_type(VALUE, stream_type=stream_type)
        return self._assume_regular(stream)

    @deprecated_with_alternative('get_one_column_values()')
    def keys(self, uniq: bool, stream_type: Union[StreamType, Auto] = AUTO) -> RegularStreamInterface:
        items = self.get_uniq_keys() if uniq else self._get_mapped_items(KEY)
        stream_type = Auto.acquire(stream_type, StreamType.AnyStream)
        stream = self.stream(items, stream_type=stream_type)
        return self._assume_regular(stream)

    def get_uniq_values(self, column=VALUE) -> Iterable:
        uniq_values = list()
        for i in self.get_one_column_values(column, as_list=False):
            if i not in uniq_values:
                uniq_values.append(i)
                yield i

    def get_uniq_keys(self, as_list: bool = False) -> Union[list, Iterable]:
        keys = self.get_uniq_values(KEY)
        if as_list:
            return list(keys)
        else:
            return keys

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

    @staticmethod
    def _assume_regular(stream) -> RegularStreamInterface:
        return stream

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
