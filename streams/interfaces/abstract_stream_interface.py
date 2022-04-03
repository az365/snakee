from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.sourced_interface import SourcedInterface
    from loggers.logger_interface import LoggerInterface, LoggingLevel
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.sourced_interface import SourcedInterface
    from ...loggers.logger_interface import LoggerInterface, LoggingLevel

Stream = SourcedInterface
Data = Union[Stream, Any]
OptionalFields = Optional[Union[Iterable, str]]


class StreamInterface(SourcedInterface, ABC):
    @classmethod
    @abstractmethod
    def get_stream_type(cls):
        """ClassMethod returns type of stream as StreamType enum-object.

        :returns: StreamType enum-object
        """
        pass

    @abstractmethod
    def set_name(self, name: str, register: bool = True, inplace: bool = False) -> Optional[Stream]:
        """Rename stream and (optionally) register it in SnakeeContext by this name.

        :param name: must be unique in your project (current SnakeeContext session)
        :param register: boolean flag: do you want to register this stream in SnakeeContext
        :param inplace: boolean flag: is it necessary to return renamed stream-object or rename it inplace w/o return
        :returns: renamed stream if inplace=True or nothing (None) if inplace=False
        """
        pass

    @abstractmethod
    def get_count(self) -> Optional[int]:
        """Returns count of items in stream if it is known.

        :returns: integer count of items if it is known, otherwise None (if this stream is over iterator)
        """
        pass

    @abstractmethod
    def get_one_item(self):
        """Returns first item from stream for example."""
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        """Returns iterable items.

        :return: list or Iterator
        """
        pass

    @abstractmethod
    def map(self, function: Callable) -> Stream:
        """Apply function to each item in stream.

        :param function: py-function that should be applied to any item (it must return an item of same type)
        :returns: stream of same type
        """
        pass

    @abstractmethod
    def filter(self, function: Callable) -> Stream:
        """Filter items by value of provided function applied on each item.

        :param function: py-function that should be applied to any item (it must return an item of same type)
        :returns: stream of same type
        """
        pass

    @abstractmethod
    def take(self, count: int) -> Stream:
        """Return stream containing first N items.
        Alias for head()

        :param count: count of items to return
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def skip(self, count: int) -> Stream:
        """Return stream instead of first N items.

        :param count: count of first items to skip
        :type count: int
        :returns: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def get_source(self) -> SourcedInterface:
        """Returns connector to data source of this stream (if defined).

        :returns: Connector to data source of this stream (if defined) or Context object.
        :return type: SourcedInterface (Connector or Context)
        """
        pass

    @abstractmethod
    def get_logger(self) -> LoggerInterface:
        """Returns current logger from SnakeeContext.

        :returns: ExtendedLogger or FallbackLogger.
        """
        pass

    @abstractmethod
    def log(
            self,
            msg: str,
            level: Union[LoggingLevel, int] = arg.AUTO,
            end: Union[str, arg.Auto] = arg.AUTO,
            verbose: bool = True,
            truncate: bool = True,
            force: bool = False,
    ) -> NoReturn:
        """Log message using current logger from SnakeeContext object.
        Do not log if logger in SnakeeContext not set (instead of enabled option force=True).

        :param msg: logged message as str
        :param level: LoggingLevel: Info, Warn, Debug, Error
        :param end: '\n' by default or '\r' for temporary message (i.e. progress status)
        :param verbose: write message to stdout in addition to log file (default is True)
        :param truncate: trim message to the length specified in ExtendedLogger settings (default is True)
        :param force: write message to stdout even logger in SnakeeContext not set (default is False)

        :returns: nothing.
        """
        pass

    @abstractmethod
    def get_data(self) -> Data:
        """Returns internal data object of stream (iterable or wrapped object)."""
        pass

    @abstractmethod
    def set_data(self, data: Data, inplace: bool) -> Optional[Stream]:
        """Replaces internal data object of Stream.
        Modifies existing stream object (if inplace=True) or returns new stream object (if inplace=False).

        :param data: new data object to set.
        :param inplace: boolean flag: set inplace or outplace

        :returns: nothing (modifies existing stream object if inplace=True) or Stream with new data (if inplace=False)
        """
        pass

    @abstractmethod
    def apply_to_data(self, function: Callable, dynamic: bool = False, *args, **kwargs) -> Stream:
        """Applies function to data, returns stream containing result value.

        :param function: function receiving internal data object as first argument
        :param dynamic: boolean sign: will the meta information be changed (i.e. count of data)
        :param args, kwargs: additional args and kwargs for function

        :returns: stream with modified data object
        """
        pass

    @abstractmethod
    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        """Extract static meta information (independent of data or items count).

        :param ex: one field name or list of fields to exclude from transmitted meta-information

        :returns: dict of meta fields and its values
        """
        pass

    @abstractmethod
    def stream(self, data: Data, ex: OptionalFields = None, **kwargs) -> Stream:
        """Returns modified stream with new data object (and probably changed meta information)
        Meta-information of initial stream will be saved by default (excluding fields from ex-argument).

        :param data: new data object to set inside new stream
        :type data: Iterable

        :param ex: one field name or list of fields to exclude from transmitted meta-information
        :type ex: list or str or None

        :param kwargs: changed meta information

        :returns: new Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def copy(self):
        """Return full copy of stream (including copy of iterator from tee_stream() method).

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def forget(self) -> NoReturn:
        """Closes related connection and lets SnakeeContext forget the link to this stream."""
        pass

    @abstractmethod
    def get_links(self) -> Iterable:
        """Returns links to related connections.
        For Stream object only one connection is possible, but get_links() returns iterable for compatibility.

        :returns: iterable with connector items (one connection item for stream)
        """
        pass
