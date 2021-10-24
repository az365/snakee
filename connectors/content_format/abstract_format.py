from abc import ABC, abstractmethod
from typing import Optional, NoReturn, Union, Iterable, Generator

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        Item, Record, Row, StructRow,
        ItemType, StreamType, FileType,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoConnector, OptionalFields, Array, ARRAY_TYPES,
    )
    from base.abstract.abstract_base import AbstractBaseObject
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        Item, Record, Row, StructRow,
        ItemType, StreamType, FileType,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoConnector, OptionalFields, Array, ARRAY_TYPES,
    )
    from ...base.abstract.abstract_base import AbstractBaseObject

ContentType = FileType
Compress = Union[str, bool, None]

DEFAULT_COMPRESS_METHOD = 'gzip'
AVAILABLE_COMPRESS_METHODS = (DEFAULT_COMPRESS_METHOD, )


class AbstractFormat(AbstractBaseObject, ABC):
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

    def copy(self):
        return self.make_new()


class BinaryFormat(AbstractFormat):
    def get_content_type(self) -> ContentType:
        return None

    def is_text(self) -> bool:
        return False

    def get_default_stream_type(self) -> Optional[StreamType]:
        return None  # StreamType.WrapperStream

    def get_default_item_type(self) -> NoReturn:
        return None

    def cab_be_stream(self) -> bool:
        return False


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


class ParsedFormat(CompressibleFormat, ABC):
    @abstractmethod
    def get_defined(self) -> CompressibleFormat:
        pass

    @abstractmethod
    def get_formatted_item(self, item: Item, item_type: Union[ItemType, arg.Auto] = AUTO) -> str:
        pass

    def get_lines(self, items: Iterable, item_type: ItemType, add_title_row: AutoBool = AUTO) -> Generator:
        if arg.is_defined(add_title_row):
            assert not add_title_row, 'title_row available in FlatStructFormat only'
        for i in items:
            yield self.get_formatted_item(i, item_type=item_type)

    @abstractmethod
    def get_parsed_line(self, line: str, item_type: Union[ItemType, arg.Auto] = AUTO) -> Item:
        pass

    def get_items(
            self,
            lines: Iterable,
            item_type: Union[ItemType, arg.Auto] = AUTO,
            **kwargs,
    ) -> Generator:
        assert not kwargs
        for i in lines:
            yield self.get_parsed_line(line=i, item_type=item_type)

    def get_stream(
            self,
            lines: Iterable,
            stream_type: Union[StreamType, arg.Auto] = AUTO,
            **kwargs
    ):
        stream_class = stream_type.get_class()
        if hasattr(stream_class, 'get_item_type'):  # isinstance(stream_class, AnyStream)
            item_type = stream_class.get_item_type()
        else:
            raise TypeError
        item_kwargs = dict(struct=kwargs['struct']) if 'struct' in kwargs else dict()
        items = self.get_items(lines, item_type=item_type, **item_kwargs)
        return stream_class(items, **kwargs)
