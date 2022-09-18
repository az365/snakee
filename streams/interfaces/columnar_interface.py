from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from utils.external import DataFrame
    from utils.algo import JoinType
    from streams.interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from streams.interfaces.regular_stream_interface import RegularStreamInterface, DEFAULT_ANALYZE_COUNT
    from streams.stream_type import StreamType
    from content.struct.struct_interface import StructInterface
    from content.items.item_type import ItemType, Item, FieldID
    from base.interfaces.context_interface import ContextInterface
    from base.classes.auto import AUTO, Auto
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.external import DataFrame
    from ...utils.algo import JoinType
    from .abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from .regular_stream_interface import RegularStreamInterface, DEFAULT_ANALYZE_COUNT
    from ..stream_type import StreamType
    from ...content.struct.struct_interface import StructInterface
    from ...content.items.item_type import ItemType, Item, FieldID
    from ...base.interfaces.context_interface import ContextInterface
    from ...base.classes.auto import AUTO, Auto

Native = RegularStreamInterface
Stream = StreamInterface
Struct = Optional[StructInterface]
Context = Optional[ContextInterface]
Columns = Union[list, tuple]
AutoBool = Union[Auto, bool]
UniKey = Union[StructInterface, Columns, FieldID, Callable]


class ColumnarInterface(RegularStreamInterface, ABC):
    @classmethod
    @abstractmethod
    def is_valid_item(cls, item: Item) -> bool:
        """Checks is provided item valid for this stream class.

        :param item: item to validate
        :return: boolean flag
        """
        pass

    @classmethod
    @abstractmethod
    def get_validated(cls, items: Iterable, skip_errors: bool = False, context: Context = None) -> Iterable:
        """Returns same items after validation by _is_valid_item() method.

        :param items: items to validate
        :param skip_errors: if `True` this function will return only validated items,
        if `False` it will raise exception if any item isn't valid
        :param context: you can provide context with common logger for log validation errors
        :return: validated items
        """
        pass

    @abstractmethod
    def validated(self, skip_errors: bool = False) -> Native:
        """Returns same stream with validated items.
        Use this method when you need to be sure that all items in stream have valid type for this type of stream.

        :param skip_errors: if `True` this method will return stream only valid items,
        if `False` it will raise exception if any item isn't valid.
        :return: stream with validated items
        """
        pass

    @abstractmethod
    def get_shape(self) -> tuple:
        """Returns tuple with 2 integers: count of items and count of columns.

        :return: tuple: items count, columns count
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Returns string with short description of stream data: count of rows and columns and list of columns

        :return: string with short description of stream data
        """
        pass

    @abstractmethod
    def get_column_count(self) -> int:
        """Returns count of columns in items of stream (fast detected by some example lines).

        :return: integer with count of columns in items
        """
        pass

    @abstractmethod
    def filter(self, *args, item_type: ItemType = ItemType.Auto, skip_errors: bool = False, **kwargs) -> Native:
        """Filter items by listed fields or values of provided functions applied on each item.

        :param item_type: expected item_type (for retrieve every filtering value correctly)
        :param skip_errors: ignore errors while calculating filtering values
        :returns: stream of the same type
        """
        pass

    @abstractmethod
    def flat_map(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def map(self, function: Callable) -> Native:
        """Apply function to each item in stream.

        :param function: py-function that should be applied to any item (it must return an item of same type)
        :returns: stream of same type
        """
        pass

    @abstractmethod
    def map_side_join(
            self,
            right: Native,
            key: UniKey,
            how: Union[JoinType, str] = JoinType.Left,
            right_is_uniq: bool = True,
    ) -> Native:
        pass

    @abstractmethod
    def sorted_group_by(self, *keys, **kwargs) -> Native:
        pass

    @abstractmethod
    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Native:
        pass

    @abstractmethod
    def apply_to_stream(self, function: Callable, *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def update_count(self) -> Native:
        pass

    @abstractmethod
    def actualize(self) -> Native:
        pass

    @abstractmethod
    def structure(self, struct: StructInterface, skip_bad_rows=False, skip_bad_values=False, verbose=True) -> Native:
        pass

    @abstractmethod
    def get_source_struct(self, default: Struct = None) -> Struct:
        pass

    @abstractmethod
    def get_detected_struct(
            self,
            count: int = DEFAULT_ANALYZE_COUNT,
            set_types: Optional[dict] = None,
            default: Struct = None,
    ) -> Struct:
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
            self,
            *filters,
            count: int = DEFAULT_EXAMPLE_COUNT,
            allow_tee_iterator: bool = True,
            allow_spend_iterator: bool = True,
            **filter_kwargs
    ) -> Native:
        pass

    @abstractmethod
    def get_demo_example(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            as_dataframe: AutoBool = AUTO,
            filters: Optional[Columns] = None, columns: Optional[Columns] = None,
    ) -> Union[DataFrame, Iterable]:
        pass

    @abstractmethod
    def show(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            filters: Columns = None,
            columns: Columns = None,
            as_dataframe: AutoBool = AUTO,
    ):
        pass

    @abstractmethod
    def describe(
            self,
            *filters,
            take_struct_from_source: bool = False,
            count: int = DEFAULT_EXAMPLE_COUNT,
            columns: Columns = None,
            show_header: bool = True,
            struct_as_dataframe: bool = False,
            separate_by_tabs: bool = False,
            allow_collect: bool = True,
            **filter_kwargs
    ):
        pass
