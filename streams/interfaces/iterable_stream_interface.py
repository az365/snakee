from abc import ABC, abstractmethod
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from streams.interfaces.iterable_interface import IterableInterface
    from streams.interfaces.abstract_stream_interface import StreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..interfaces.iterable_interface import IterableInterface
    from ..interfaces.abstract_stream_interface import StreamInterface

Native = StreamInterface
Stream = StreamInterface


class IterableStreamInterface(StreamInterface, IterableInterface, ABC):
    @abstractmethod
    def get_count(self) -> Optional[int]:
        """Returns count of items in stream if it's known, otherwise returns None.

        :returns: integer count of items if it is known, otherwise None (if this stream is over iterator)
        """
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
    def close(self, recursively: bool = False, return_closed_links: bool = False) -> Union[int, tuple]:
        """Closes stream and its sources by known links (i.e. file or database connection).

        :param recursively: close all links to stream recursively
        :type recursively: bool

        :param return_closed_links: let return count of closed streams and links, otherwise stream count only
        :type return_closed_links: bool

        :return: count of closed streams or tuple with count of closed streams and links
        """
        pass
