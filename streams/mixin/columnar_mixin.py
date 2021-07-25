from abc import ABC, abstractmethod
from typing import Union, Iterable, Generator, Callable, Any, Optional

try:  # Assume we're a sub-module in a package.
    from interfaces import (
        Stream, RegularStream, RegularStreamInterface, SchemaStream, SchemaInterface,
        StreamType, ItemType,
        Count, UniKey, Item, Array, Columns, OptionalFields,
        AUTO, Auto, AutoBool,
    )
    from utils import algo, arguments as arg, mappers as ms
    from utils.external import pd, DataFrame, get_use_objects_for_output
    from fields import field_classes as fc
    from base.abstract.contextual_data import ContextualDataWrapper
    from functions import item_functions as fs
    from utils import selection as sf
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Stream, RegularStream, RegularStreamInterface, SchemaStream, SchemaInterface,
        StreamType, ItemType,
        Count, UniKey, Item, Array, Columns, OptionalFields,
        AUTO, Auto, AutoBool,
    )
    from ...utils import algo, arguments as arg, mappers as ms
    from ...utils.external import pd, DataFrame, get_use_objects_for_output
    from ...fields import field_classes as fc
    from ...base.abstract.contextual_data import ContextualDataWrapper
    from ...functions import item_functions as fs
    from ...utils import selection as sf

Native = RegularStream
Schema = SchemaInterface

SAFE_COUNT_ITEMS_IN_MEMORY = 10000
EXAMPLE_STR_LEN = 12


class ColumnarInterface(RegularStreamInterface, ABC):
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
    def filter(self, *args, **kwargs) -> Stream:
        pass

    @abstractmethod
    def map(self, function: Callable) -> Stream:
        pass

    @abstractmethod
    def map_side_join(self, right: Stream, key: UniKey, how: str = 'left', right_is_uniq: bool = True) -> Stream:
        pass

    @abstractmethod
    def select(self, *fields, **expressions) -> Stream:
        pass

    @abstractmethod
    def sort(self, *keys, reverse: bool = False) -> Stream:
        pass

    @abstractmethod
    def sorted_group_by(self, *keys, values: OptionalFields = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def group_by(self, *keys, values: OptionalFields = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def is_in_memory(self) -> bool:
        pass

    @abstractmethod
    def get_dataframe(self, columns: Columns = None) -> DataFrame:
        pass

    @abstractmethod
    def show(self, count: int = 10, filters: OptionalFields = None, columns: OptionalFields = None):
        pass

    @abstractmethod
    def apply_to_stream(self, function: Callable, *args, **kwargs) -> Stream:
        pass

    @abstractmethod
    def get_one_item(self) -> Item:
        pass

    @abstractmethod
    def update_count(self) -> Stream:
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

    def validated(self, skip_errors=False) -> Native:
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

    def filter(self, *args, item_type: ItemType = ItemType.Auto, skip_errors: bool = False, **kwargs) -> Native:
        stream = self.stream(
            sf.filter_items(*args, item_type=item_type, skip_errors=skip_errors, logger=self.get_logger(), **kwargs),
        )
        return self._assume_native(stream)

    def _get_flat_mapped_items(self, function: Callable) -> Generator:
        for i in self.get_items():
            yield from function(i)

    def flat_map(self, function: Callable) -> Native:
        stream = self.stream(
            self._get_flat_mapped_items(function=function),
        )
        return self._assume_native(stream)

    def map_to(self, function: Callable, stream_type: StreamType) -> Stream:
        return self.stream(
            map(function, self.get_items()),
            stream_type=stream_type,
        )

    def map(self, function: Callable) -> Native:
        stream = self.stream(
            map(function, self.get_items()),
        )
        return self._assume_native(stream)

    def map_side_join(self, right: Native, key: UniKey, how='left', right_is_uniq=True) -> Native:
        assert how in algo.JOIN_TYPES, 'only {} join types are supported ({} given)'.format(algo.JOIN_TYPES, how)
        keys = arg.update([key])
        joined_items = algo.map_side_join(
            iter_left=self.get_items(),
            iter_right=right.get_items(),
            key_function=fs.composite_key(keys),
            merge_function=ms.merge_two_items,
            dict_function=ms.items_to_dict,
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
        keys = arg.get_names(keys)
        keys = arg.update(keys)
        values = arg.get_names(values)
        return self.sort(*keys).sorted_group_by(*keys, values=values, as_pairs=as_pairs)

    def apply_to_stream(self, function: Callable, *args, **kwargs) -> Stream:
        return function(self, *args, **kwargs)

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def update_count(self) -> Native:
        assert self.is_in_memory()
        count = len(self.get_list())
        self.set_expected_count(count)
        self.set_estimated_count(count)
        return self

    def get_str_count(self) -> str:  # tmp?
        return str(self.get_count())

    def actualize(self) -> Native:
        source = self.get_source()
        if hasattr(source, 'actualize'):
            source.actualize()
        if self.get_count():
            if self.get_count() < SAFE_COUNT_ITEMS_IN_MEMORY:
                self.collect(inplace=True)
        self.update_count()
        return self

    def get_source_schema(self, default=None) -> Optional[Schema]:
        source = self.get_source()
        if hasattr(source, 'get_schema'):
            return source.get_schema()
        else:
            return default

    def get_detected_struct(self, count: int = 100, set_types: Optional[dict] = None, default=None) -> Optional[Schema]:
        if hasattr(self, 'get_detected_columns'):
            columns = self.get_detected_columns(count)
        else:
            columns = self.get_columns()
        struct = fc.FieldGroup.get_schema_detected_by_title_row(columns)
        if struct:
            if set_types:
                struct.set_types(set_types)
            return struct
        else:
            return default

    def get_dataframe(self, columns: Optional[Iterable] = None) -> DataFrame:
        df = DataFrame(self.get_data())
        if df and columns:
            df = df[columns]
        return df

    def get_str_description(self) -> str:
        return '{} rows, {} columns: {}'.format(
            self.get_str_count(),
            self.get_columns_count(),
            ', '.join(self.get_columns()),
        )

    def get_str_headers(self) -> Iterable:
        yield '{}({}) {}'.format(self.__class__.__name__, self.get_name(), self.get_str_description())

    def get_one_item(self):
        one_item_stream = self.take(1)
        assert isinstance(one_item_stream, RegularStreamInterface)
        list_one_item = list(one_item_stream.get_items())
        if list_one_item:
            return list_one_item[0]
        else:
            return None

    def example(
            self, *filters, count: int = 10,
            allow_tee_iterator: bool = True,
            allow_spend_iterator: bool = True,
            **filter_kwargs
    ):
        self.actualize()
        use_tee = allow_tee_iterator and hasattr(self, 'tee_stream')
        if self.is_in_memory() or (allow_spend_iterator and not use_tee):
            example = self.filter(*filters, **filter_kwargs).take(count)
        elif use_tee and hasattr(self, 'tee_stream'):
            assert hasattr(self, 'tee_stream')
            example = self.tee_stream().take(count)
        else:  # keep safe items in iterator
            example = self.take(1)
        return self._assume_native(example)

    def get_demo_example(
            self, count: int = 10,
            as_dataframe: AutoBool = AUTO,
            filters: Optional[Array] = None, columns: Optional[Array] = None,
    ):
        sm_sample = self.filter(*filters or []) if filters else self
        sm_sample = sm_sample.take(count)
        if hasattr(sm_sample, 'get_dataframe'):
            return sm_sample.get_dataframe(columns)
        elif hasattr(sm_sample, 'select') and columns:
            return sm_sample.select(*columns).get_items()
        elif hasattr(sm_sample, 'get_items'):
            return sm_sample.get_items()

    def show(self, count: int = 10, filters: Columns = None, columns: Columns = None, as_dataframe: AutoBool = AUTO):
        self.log(self.get_str_description(), truncate=False, force=True)
        return self.get_demo_example(count=count, filters=filters, columns=columns)

    def describe(
            self, *filters,
            take_struct_from_source: bool = False,
            count: Count = 10, columns: Columns = None,
            show_header: bool = True, struct_as_dataframe: bool = False, separate_by_tabs: bool = False,
            allow_collect: bool = True, **filter_kwargs
    ):
        if show_header:
            for line in self.get_str_headers():
                self.log(line)
        example = self.example(*filters, **filter_kwargs, count=count)
        assert isinstance(example, ColumnarMixin)
        if hasattr(self, 'get_schema'):
            struct = self.get_schema()
            source_str = 'native'
        elif take_struct_from_source:
            struct = self.get_source_schema()
            source_str = 'from source {}'.format(self.get_source().__repr__())
        else:
            struct = self.get_detected_struct()
            source_str = 'detected from example items'
        struct = fc.FieldGroup.convert_to_native(struct)
        struct.validate_about(example.get_detected_struct(count))
        message = '{} {}'.format(source_str, struct.get_validation_message())
        struct_as_dataframe = struct_as_dataframe and get_use_objects_for_output()
        struct_dataframe = struct.describe(
            as_dataframe=struct_as_dataframe, show_header=False, logger=self.get_logger(),
            separate_by_tabs=separate_by_tabs, example=example.get_one_item(), comment=message,
        )
        if struct_as_dataframe:
            return struct_dataframe
        else:
            return example.get_demo_example(as_dataframe=get_use_objects_for_output())
