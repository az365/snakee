from abc import ABC, abstractmethod
from typing import Optional, Iterable, Sequence, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from utils.external import DataFrame
    from content.struct.struct_interface import StructInterface, Field
    from content.items.item_type import ItemType
    from streams.stream_type import StreamType
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...utils.external import DataFrame
    from ...content.struct.struct_interface import StructInterface, Field
    from ...content.items.item_type import ItemType
    from ..stream_type import StreamType
    from ..interfaces.iterable_stream_interface import IterableStreamInterface

Stream = IterableStreamInterface
Struct = Optional[StructInterface]
OptionalFields = Union[Iterable, str, None]


class RegularStreamInterface(IterableStreamInterface, ABC):
    @staticmethod
    def get_item_type() -> ItemType:
        """Returns ItemType object, representing expected type of iterable data

        :return: ItemType object
        """
        return ItemType.Any

    @abstractmethod
    def get_struct(self) -> Struct:
        pass

    @abstractmethod
    def map_to_type(self, function: Callable, stream_type: StreamType, **kwargs) -> Stream:
        """Apply function to each item in stream.

        :param function: py-function that should be applied to any item (it must return an item of same type)
        :param stream_type: type of output stream (deprecated, use item_type: ItemType instead)
        :returns: stream of requested type
        """
        pass

    @abstractmethod
    def select(self, *columns, **expressions) -> Stream:
        pass

    @abstractmethod
    def sorted_group_by(
            self,
            *keys,
            values: Optional[Iterable] = None,
            as_pairs: bool = False,
    ) -> Stream:
        pass

    @abstractmethod
    def group_by(
            self,
            *keys,
            values: Optional[Iterable] = None,
            as_pairs: bool = False,
            take_hash: bool = True,
            step: Union[int, Auto] = AUTO,
            verbose: bool = True,
    ) -> Stream:
        pass

    @abstractmethod
    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        pass

    @abstractmethod
    def get_dict(
            self,
            key: Union[Field, Struct, Sequence, Callable, Auto] = AUTO,
            value: Union[Field, Struct, Sequence, Callable, Auto] = AUTO,
            of_lists: bool = False,
            skip_errors: bool = False,
    ) -> dict:
        """Aggregate stream data into python dictionary.
        Using key and value arguments for get key and value from each item of stream.

        :param key: field, struct or function for get key from each item.
        :param value: field, struct or function for get value from each item.
        :param of_lists: collect list of all values for value-field.
        :param skip_errors: skip errors (missing field) when occurred (otherwise raise error).
        :returns: dict
        """
        pass

    @abstractmethod
    def get_dataframe(self, columns: Optional[Iterable] = None) -> DataFrame:
        """Converts full stream data to Pandas DataFrame.
        Can use subset of columns and define order of columns (if columns-argument provided).
        Pandas must be installed, otherwise raise exception.

        :param columns: list of required fields (columns) or None (take all columns in arbitrary orders).
        :returns: Pandas DataFrame
        """
        pass
