from abc import ABC, abstractmethod
from typing import Optional, Union, Callable, Iterable, Iterator, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import DataFrame
    from utils.algo import JoinType
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from base.interfaces.context_interface import ContextInterface
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import DataFrame
    from ...utils.algo import JoinType
    from ..stream_type import StreamType
    from ..interfaces.abstract_stream_interface import StreamInterface
    from ...base.interfaces.context_interface import ContextInterface
    from ...loggers.selection_logger_interface import SelectionLoggerInterface

Native = StreamInterface
Stream = StreamInterface
Context = Union[ContextInterface, arg.Auto, None]
Count = Union[int, arg.Auto]
OptionalFields = Union[Iterable, str, None]


class IterableStreamInterface(StreamInterface, ABC):
    @abstractmethod
    def __iter__(self) -> Iterable:
        """Returns link to Iterable data in stream.
        :return: list or Iterator
        """
        pass

    @abstractmethod
    def get_iter(self) -> Iterator:
        """Presents items from stream as Iterator.
        :return: Iterator or Generator
        """
        pass

    @abstractmethod
    def get_count(self) -> Optional[int]:
        """Returns count of items in stream is it's known, otherwise returns None.
        :return: int or None
        """
        pass

    @abstractmethod
    def is_in_memory(self) -> bool:
        """Checks is the data of stream in RAM or in external iterator.
        :return: True if stream has data as Sequence in memory, False if it has an iterator
        """
        pass

    @abstractmethod
    def close(self, recursively: bool = False, return_closed_links: bool = False) -> Union[int, tuple]:
        """Closes stream and its sources by known links (i.e. file or database connection).

        :param recursively: close all links to stream recursively
        :type recursively: bool

        :param return_closed_links: let return count of closed streams and links, otherwise stream count only
        :type return_closed_links: bool

        :return: count of closed streams or tuple with count of closed streams and links
        """
        pass

    @abstractmethod
    def forget(self) -> NoReturn:
        """Closes stream and remove links to it from SnakeeContext and connectors."""
        pass

    @abstractmethod
    def get_expected_count(self) -> Optional[int]:
        """Returns expected count of items if it's provided in from stream meta-information.

        :return: int or None (if count expectation is not available for this stream or data source)
        """
        pass

    @abstractmethod
    def get_estimated_count(self) -> Optional[int]:
        """Returns estimated count (upper bound) of items from stream meta-information.

        :return: int or None (if count estimation is not available for this stream or data source)
        """
        pass

    @abstractmethod
    def get_str_count(self) -> str:
        """Returns string with general information about expected and estimated count of items in stream."""
        pass

    @abstractmethod
    def enumerate(self, native: bool = False) -> Stream:
        """Returns stream with enumerated items of current stream.

        :param native: let return stream of same class (KeyValueStream will returned by default)
        :type native: bool

        :return: KeyValueStream (if native=False) or stream of same class (if native=True)
        """
        pass

    @abstractmethod
    def get_one_item(self):
        """Returns first item from stream for example."""
        pass

    @abstractmethod
    def take(self, count: int = 1) -> Native:
        """Return stream containing first N items.
        Alias for head()

        :param count: count of items to return
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def head(self, count: int = 10) -> Native:
        """Return stream containing first N items.
        Alias for take()

        :param count: count of items to return
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def tail(self, count: int = 10) -> Native:
        """Return stream containing last N items from current stream.

        :param count: count of items to return
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def skip(self, count: int) -> Native:
        """Return stream with items except first N items.

        :param count: count of items to skip
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def pass_items(self) -> Native:
        """Receive and skip all items from data source.
        Can be used for case when data source must be sure that all data has been transmitted successfully.

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def tee_stream(self) -> Native:
        """Return stream with copy of initial iterator.
        Current stream save previous iterator position.
        Uses tee() functions from python itertools: https://docs.python.org/3/library/itertools.html#itertools.tee

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def stream(self, data: Iterable, ex: OptionalFields = None, **kwargs) -> Native:
        """Build new stream with data provided.
        Meta-information of initial stream will by saved by default (excluding fields from ex-argument).

        :param data: link to iterable data for new stream
        :type data: Iterable

        :param ex: one field name or list of fields to exclude from transmitted meta-information
        :type ex: list or str or None

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def copy(self) -> Native:
        """Return full copy of stream (including copy of iterator from tee_stream() method)."""
        pass

    @abstractmethod
    def add(self, stream_or_items: Union[Native, Iterable], before=False, **kwargs) -> Native:
        pass

    @abstractmethod
    def add_stream(self, stream: Native, before: bool = False) -> Native:
        pass

    @abstractmethod
    def add_items(self, items: Iterable, before: bool = False) -> Native:
        pass

    @abstractmethod
    def split(self, by: Union[int, list, tuple, Callable], count: Optional[int] = None) -> Iterable:
        pass

    @abstractmethod
    def split_to_iter_by_step(self, step: int) -> Iterable:
        pass

    @abstractmethod
    def flat_map(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def map_side_join(
            self,
            right: Native,
            key,
            how: Union[JoinType, str] = JoinType.Left,
            right_is_uniq: bool = True,
    ) -> Native:
        pass

    @abstractmethod
    def apply_to_data(
            self, function: Callable,
            to: StreamType = arg.AUTO,
            save_count: bool = False, lazy: bool = True,
    ) -> Stream:
        pass

    @abstractmethod
    def progress(
            self,
            expected_count: Count = arg.AUTO, step: Count = arg.AUTO,
            message: str = 'Progress',
    ) -> Native:
        """Shows customizable progress-bar on output, writes logs of progress into file, if file added to logger.

        :param expected_count: allows to provide expected count of items in stream, when it's known
        :param step: how often show update (lower values can make process more slow), 10000 by default
        :param message: custom message to show in progress-bar
        :return: same stream
        """
        pass

    @abstractmethod
    def print(self, stream_function: Union[str, Callable] = '_count', *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def submit(
            self,
            external_object: Union[list, dict, Callable] = print,
            stream_function: Union[Callable, str] = 'get_count',
            key: Optional[str] = None,
            show: bool = False,
    ) -> Stream:
        pass

    @abstractmethod
    def set_meta(self, **meta) -> Native:
        pass

    @abstractmethod
    def update_meta(self, **meta) -> Native:
        pass

    @abstractmethod
    def get_selection_logger(self) -> SelectionLoggerInterface:
        pass

    @abstractmethod
    def get_dataframe(self, columns: Optional[Iterable] = None) -> DataFrame:
        pass
