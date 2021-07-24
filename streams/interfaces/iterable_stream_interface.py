from abc import ABC, abstractmethod
from typing import Optional, Union, Callable, Iterable, Iterator, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import DataFrame
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from base.interfaces.context_interface import ContextInterface
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import DataFrame
    from ..stream_type import StreamType
    from ..interfaces.abstract_stream_interface import StreamInterface
    from ...base.interfaces.context_interface import ContextInterface
    from ...loggers.selection_logger_interface import SelectionLoggerInterface

Native = StreamInterface
Stream = StreamInterface
Context = Union[ContextInterface, arg.Auto, None]
Count = Union[int, arg.Auto]
OptionalFields = Union[Iterable, str, None]
How = str


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

    @classmethod
    def is_valid_item_type(cls, item) -> bool:
        pass

    @abstractmethod
    def is_valid_item(self, item) -> bool:
        pass

    @classmethod
    @abstractmethod
    def get_typing_validated_items(cls, items, skip_errors: bool = False, context: Context = None) -> Iterable:
        pass

    @abstractmethod
    def get_validated_items(self, items, skip_errors: bool = False, context: Context = None) -> Iterable:
        pass

    @abstractmethod
    def is_in_memory(self) -> bool:
        pass

    @abstractmethod
    def close(self, recursively: bool = False, return_closed_links: bool = False) -> Union[int, tuple]:
        pass

    @abstractmethod
    def forget(self) -> NoReturn:
        pass

    @abstractmethod
    def get_expected_count(self) -> Optional[int]:
        pass

    @abstractmethod
    def one(self):
        pass

    @abstractmethod
    def get_estimated_count(self) -> Optional[int]:
        pass

    @abstractmethod
    def get_str_count(self) -> str:
        pass

    @abstractmethod
    def enumerate(self, native: bool = False) -> Stream:
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
        pass

    @abstractmethod
    def skip(self, count: int = 1) -> Native:
        pass

    @abstractmethod
    def pass_items(self) -> Native:
        pass

    @abstractmethod
    def tee_stream(self) -> Native:
        pass

    @abstractmethod
    def stream(self, data: Iterable, ex: OptionalFields = None, **kwargs) -> Native:
        pass

    @abstractmethod
    def copy(self) -> Native:
        return self.tee_stream()

    @abstractmethod
    def add(self, stream_or_items: Union[Native, Iterable], before=False, **kwargs) -> Native:
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
    def map_side_join(self, right: Native, key, how: How = 'left', right_is_uniq: bool = True) -> Native:
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
        :return:
        """
        pass

    @abstractmethod
    def print(self, stream_function: Union[str, Callable] = '_count', *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def submit(
            self,
            external_object: Union[list, dict, Callable] = print,
            stream_function: Union[Callable, str] = 'count',
            key: Optional[str] = None, show=False,
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
