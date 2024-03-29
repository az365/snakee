from abc import ABC, abstractmethod
from typing import Optional, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import ContentFormatInterface, ContentType, StreamType, ItemType, Item
    from base.abstract.abstract_base import AbstractBaseObject
    from streams.stream_builder import StreamBuilder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import ContentFormatInterface, ContentType, StreamType, ItemType, Item
    from ...base.abstract.abstract_base import AbstractBaseObject
    from ...streams.stream_builder import StreamBuilder

Compress = Union[str, bool, None]

DEFAULT_COMPRESS_METHOD = 'gzip'
AVAILABLE_COMPRESS_METHODS = (DEFAULT_COMPRESS_METHOD, )
META_MEMBER_MAPPING = dict(_compress_method='compress')


class AbstractFormat(AbstractBaseObject, ContentFormatInterface, ABC):
    def is_binary(self) -> bool:
        return not self.is_text()

    def copy(self):
        return self.make_new()


class BinaryFormat(AbstractFormat):
    def get_content_type(self) -> Optional[ContentType]:
        return None

    def is_text(self) -> bool:
        return False

    def get_default_stream_type(self) -> Optional[StreamType]:
        return None  # StreamType.WrapperStream

    def get_default_item_type(self) -> Optional[ItemType]:
        return None

    def cab_be_stream(self) -> bool:
        return False

    def get_lines(self, items: Iterable, item_type: ItemType, add_title_row: Optional[bool] = None) -> Generator:
        raise NotImplementedError


class CompressibleFormat(AbstractFormat, ABC):
    def __init__(self, compress: Compress = None):
        if compress:
            if isinstance(compress, bool):
                compress = DEFAULT_COMPRESS_METHOD
            elif isinstance(compress, str):
                assert compress in AVAILABLE_COMPRESS_METHODS
            else:
                raise TypeError
        self._compress_method = compress

    @staticmethod
    def get_default_compress_method() -> str:
        return DEFAULT_COMPRESS_METHOD

    def get_compress_method(self) -> Optional[str]:
        return self._compress_method

    def is_compressed(self) -> bool:
        return bool(self.get_compress_method())

    def is_gzip(self) -> bool:
        return self.get_compress_method() == 'gzip'

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return META_MEMBER_MAPPING


class ParsedFormat(CompressibleFormat, ABC):
    @abstractmethod
    def get_defined(self) -> CompressibleFormat:
        pass

    @abstractmethod
    def get_formatted_item(self, item: Item, item_type: ItemType = ItemType.Auto) -> str:
        pass

    def get_lines(self, items: Iterable, item_type: ItemType, add_title_row: Optional[bool] = None) -> Generator:
        if add_title_row is not None:
            assert not add_title_row, 'title_row available in FlatStructFormat only'
        for i in items:
            yield self.get_formatted_item(i, item_type=item_type)

    @abstractmethod
    def get_parsed_line(self, line: str, item_type: ItemType = ItemType.Auto) -> Item:
        pass

    def get_items_from_lines(
            self,
            lines: Iterable,
            item_type: ItemType = ItemType.Auto,
            **kwargs,
    ) -> Generator:
        assert not kwargs
        for i in lines:
            yield self.get_parsed_line(line=i, item_type=item_type)

    def get_stream(
            self,
            lines: Iterable,
            item_type: ItemType = ItemType.Auto,
            **kwargs
    ):
        item_kwargs = dict(struct=kwargs['struct']) if 'struct' in kwargs else dict()
        items = self.get_items_from_lines(lines, item_type=item_type, **item_kwargs)
        return StreamBuilder.stream(items, item_type=item_type, **kwargs)
