from abc import ABC, abstractmethod
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from base.interfaces.base_interface import BaseInterface
    from items.item_type import ItemType
    from streams.stream_type import StreamType
    from content.format.content_type import ContentType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.base_interface import BaseInterface
    from ...items.item_type import ItemType
    from ...streams.stream_type import StreamType
    from ...content.format.content_type import ContentType

Compress = Union[str, bool, None]

DEFAULT_COMPRESS_METHOD = 'gzip'
AVAILABLE_COMPRESS_METHODS = (DEFAULT_COMPRESS_METHOD, )
META_MEMBER_MAPPING = dict(_compress_method='compress')


class ContentFormatInterface(BaseInterface, ABC):
    @abstractmethod
    def get_content_type(self) -> Optional[ContentType]:
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
        pass

    @abstractmethod
    def get_default_item_type(self) -> Optional[ItemType]:
        pass

    @abstractmethod
    def copy(self):
        pass
