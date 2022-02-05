from abc import ABC, abstractmethod
from typing import Optional, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.classes.typing import AutoBool
    from base.interfaces.base_interface import BaseInterface
    from streams.stream_type import StreamType
    from content.items.item_type import ItemType
    from content.format.content_type import ContentType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import Auto, AUTO
    from ...base.classes.typing import AutoBool
    from ...base.interfaces.base_interface import BaseInterface
    from ...streams.stream_type import StreamType
    from ..items.item_type import ItemType
    from .content_type import ContentType

Compress = Union[str, bool, None]

DEFAULT_COMPRESS_METHOD = 'gzip'
AVAILABLE_COMPRESS_METHODS = (DEFAULT_COMPRESS_METHOD, )
META_MEMBER_MAPPING = dict(_compress_method='compress')


class ContentFormatInterface(BaseInterface, ABC):
    @abstractmethod
    def get_content_type(self) -> Optional[ContentType]:
        """Returns ContentType detected from file format settings."""
        pass

    @abstractmethod
    def is_text(self) -> bool:
        pass

    def is_binary(self) -> bool:
        return not self.is_text()

    @abstractmethod
    def cab_be_stream(self) -> bool:
        pass

    @abstractmethod
    def get_default_stream_type(self) -> Optional[StreamType]:
        """Returns default (recommended) StreamType for this type of file/content."""
        pass

    @abstractmethod
    def get_default_item_type(self) -> Optional[ItemType]:
        """Returns ItemType expected while parsing this file/content."""
        pass

    @abstractmethod
    def copy(self):
        pass

    @abstractmethod
    def get_lines(self, items: Iterable, item_type: ItemType, add_title_row: AutoBool = AUTO) -> Generator:
        """Get raw (not parsed) lines from file.

        :returns: Generator of strings with raw (not parsed) lines from file.
        """
        pass
