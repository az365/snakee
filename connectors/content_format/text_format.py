import json
from typing import Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        Item, Record, Row, StructRow,
        ItemType, StreamType, ContentType,
        ARRAY_TYPES, AUTO, Auto,
    )
    from connectors.content_format.abstract_format import AbstractFormat, ParsedFormat, Compress
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        Item, Record, Row, StructRow,
        ItemType, StreamType, ContentType,
        ARRAY_TYPES, AUTO, Auto,
    )
    from .abstract_format import AbstractFormat, ParsedFormat, Compress

DEFAULT_ENDING = '\n'
DEFAULT_ENCODING = 'utf8'


class TextFormat(ParsedFormat):
    def __init__(
            self,
            ending: str = DEFAULT_ENDING,
            encoding: str = DEFAULT_ENCODING,
            compress: Compress = None,
    ):
        assert isinstance(ending, str)
        self._ending = ending
        self._encoding = encoding
        super().__init__(compress=compress)

    def get_content_type(self) -> ContentType:
        return ContentType.TextFile

    def get_encoding(self) -> str:
        return self._encoding

    def get_ending(self) -> str:
        return self._ending

    def is_text(self) -> bool:
        return True

    def cab_be_stream(self) -> bool:
        return True

    @staticmethod
    def can_be_parsed() -> bool:
        return False

    @staticmethod
    def is_columnar() -> bool:
        return False

    def get_default_stream_type(self) -> StreamType:
        return StreamType.LineStream

    def get_default_item_type(self) -> ItemType:
        return ItemType.Line

    def get_defined(self) -> ParsedFormat:
        return self

    def get_formatted_item(self, item: Item, item_type: Union[ItemType, arg.Auto] = AUTO) -> str:
        return str(item)

    def get_parsed_line(self, line: str, item_type: Union[ItemType, arg.Auto] = AUTO) -> Item:
        item_type = arg.delayed_acquire(item_type, self.get_default_item_type)
        if item_type in (ItemType.Line, ItemType.Any, ItemType.Auto):
            return line
        elif item_type == ItemType.Row:
            return [line]
        elif item_type == ItemType.Record:
            return dict(line=line)
        elif item_type == ItemType.StructRow:
            return ItemType.StructRow.build(data=[line], struct=['line'])
        else:
            msg = 'item_type {} is not supported for {}.parse_lines()'
            raise ValueError(msg.format(item_type, self.__class__.__name__))

    def __repr__(self):
        return super().__repr__().replace('\t', '\\t').replace('\n', '\\n')


class JsonFormat(TextFormat):
    def __init__(
            self,
            ending: str = DEFAULT_ENDING,
            encoding: str = DEFAULT_ENCODING,
            compress: Compress = None,
    ):
        super().__init__(ending=ending, encoding=encoding, compress=compress)

    def get_content_type(self) -> ContentType:
        return ContentType.JsonFile

    @staticmethod
    def can_be_parsed() -> bool:
        return True

    def get_default_stream_type(self) -> StreamType:
        return StreamType.RecordStream

    def get_default_item_type(self) -> ItemType:
        return ItemType.Record

    def get_formatted_item(self, item: Item, item_type: Union[ItemType, arg.Auto] = AUTO) -> str:
        return json.dumps(item)

    @staticmethod
    def _parse_json_line(line: str, default_value=None):
        try:
            return json.loads(line)
        except json.JSONDecodeError as err:
            if default_value is not None:
                return default_value
            else:
                raise json.JSONDecodeError(err.msg, err.doc, err.pos)

    def get_parsed_line(self, line: str, item_type: Union[ItemType, arg.Auto] = AUTO, default_value=None) -> Item:
        item_type = arg.delayed_acquire(item_type, self.get_default_item_type)
        if item_type in (ItemType.Record, ItemType.Row, ItemType.Any, ItemType.Auto):
            parsed = self._parse_json_line(line, default_value=default_value)
            if isinstance(parsed, ARRAY_TYPES) and item_type == ItemType.Record:
                return dict(item=parsed)
            elif isinstance(parsed, dict) and item_type == ItemType.Row:
                return [parsed]
            else:
                return parsed
        elif item_type == ItemType.Line:
            return line
        elif item_type == ItemType.StructRow:
            return ItemType.StructRow.build(data=[line], struct=['line'])
        else:
            msg = 'item_type {} is not supported for {}.parse_lines()'
            raise ValueError(msg.format(item_type, self.__class__.__name__))
