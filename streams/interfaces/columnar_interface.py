from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import DataFrame
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.stream_type import StreamType
    from items.struct_interface import StructInterface
    from items.item_type import ItemType, Item, FieldID
    from base.interfaces.context_interface import ContextInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import DataFrame
    from .abstract_stream_interface import StreamInterface
    from .regular_stream_interface import RegularStreamInterface
    from ..stream_type import StreamType
    from ...items.struct_interface import StructInterface
    from ...items.item_type import ItemType, Item, FieldID
    from ...base.interfaces.context_interface import ContextInterface

Native = RegularStreamInterface
Stream = StreamInterface
Struct = Optional[StructInterface]
Context = Optional[ContextInterface]
Columns = Union[list, tuple]
AutoBool = Union[arg.Auto, bool]
UniKey = Union[StructInterface, Columns, FieldID, Callable]
Auto = arg.Auto

AUTO = arg.AUTO
LINES_COUNT_FOR_SHOW_EXAMPLE = 10
LINES_COUNT_FOR_DETECT_STRUCT = 100


class ColumnarInterface(RegularStreamInterface, ABC):
    @classmethod
    @abstractmethod
    def is_valid_item(cls, item: Item) -> bool:
        pass

    @classmethod
    @abstractmethod
    def get_validated(cls, items: Iterable, skip_errors: bool = False, context: Context = None):
        pass

    @abstractmethod
    def validated(self, skip_errors: bool = False) -> Native:
        pass

    @abstractmethod
    def get_shape(self) -> tuple:
        pass

    @abstractmethod
    def get_description(self) -> str:
        pass

    @abstractmethod
    def get_column_count(self) -> int:
        pass

    @abstractmethod
    def filter(self, *args, item_type: ItemType = ItemType.Auto, skip_errors: bool = False, **kwargs) -> Native:
        pass

    @abstractmethod
    def flat_map(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def map_to(self, function: Callable, stream_type: StreamType) -> Stream:
        pass

    @abstractmethod
    def map(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def map_side_join(self, right: Native, key: UniKey, how='left', right_is_uniq=True) -> Native:
        pass

    @abstractmethod
    def sorted_group_by(self, *keys, **kwargs) -> Stream:
        pass

    @abstractmethod
    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def apply_to_stream(self, function: Callable, *args, **kwargs) -> Stream:
        pass

    @staticmethod
    def _assume_native(stream) -> Native:
        pass

    @abstractmethod
    def update_count(self) -> Native:
        pass

    @abstractmethod
    def actualize(self) -> Native:
        pass

    @abstractmethod
    def get_source_struct(self, default: Struct = None) -> Struct:
        pass

    @abstractmethod
    def get_detected_struct(
            self,
            count: int = LINES_COUNT_FOR_DETECT_STRUCT,
            set_types: Optional[dict] = None,
            default: Struct = None,
    ) -> Struct:
        pass

    def get_dataframe(self, columns: Optional[Columns] = None) -> DataFrame:
        pass

    @abstractmethod
    def get_str_description(self) -> str:
        pass

    @abstractmethod
    def get_str_headers(self) -> Iterable:
        pass

    @abstractmethod
    def get_one_item(self) -> Optional[Item]:
        pass

    @abstractmethod
    def example(
            self, *filters, count: int = LINES_COUNT_FOR_SHOW_EXAMPLE,
            allow_tee_iterator: bool = True,
            allow_spend_iterator: bool = True,
            **filter_kwargs
    ) -> Native:
        pass

    @abstractmethod
    def get_demo_example(
            self, count: int = LINES_COUNT_FOR_SHOW_EXAMPLE,
            as_dataframe: AutoBool = AUTO,
            filters: Optional[Columns] = None, columns: Optional[Columns] = None,
    ) -> Union[DataFrame, Iterable]:
        pass

    @abstractmethod
    def show(
            self, count: int = LINES_COUNT_FOR_SHOW_EXAMPLE,
            filters: Columns = None,
            columns: Columns = None,
            as_dataframe: AutoBool = AUTO,
    ):
        pass

    @abstractmethod
    def describe(
            self, *filters,
            take_struct_from_source: bool = False,
            count: int = LINES_COUNT_FOR_SHOW_EXAMPLE,
            columns: Columns = None,
            show_header: bool = True,
            struct_as_dataframe: bool = False,
            separate_by_tabs: bool = False,
            allow_collect: bool = True,
            **filter_kwargs
    ):
        pass
