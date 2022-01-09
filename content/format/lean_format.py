from typing import Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from interfaces import Item, ItemType, StreamType, FileType, ContentType, StructInterface, Auto, AUTO
    from content.format.abstract_format import ParsedFormat, Compress
    from content.format.text_format import TextFormat, JsonFormat
    from content.format.columnar_format import ColumnarFormat, FlatStructFormat
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import Item, ItemType, StreamType, FileType, ContentType, StructInterface, Auto, AUTO
    from .content_type import ContentType
    from .abstract_format import ParsedFormat, Compress
    from .text_format import TextFormat, JsonFormat
    from .columnar_format import ColumnarFormat, FlatStructFormat

TEXT_TYPES = (
    ContentType.TextFile, ContentType.JsonFile, ContentType.ColumnFile, ContentType.CsvFile, ContentType.TsvFile,
    FileType.TextFile, FileType.JsonFile, FileType.ColumnFile, FileType.CsvFile, FileType.TsvFile,
)


class LeanFormat(ParsedFormat):
    def __init__(self, content_type: ContentType, compress: Compress = None, **options):
        self._content_type = content_type
        self._options = options
        super().__init__(compress=compress)

    def get_content_type(self) -> ContentType:
        return self._content_type

    def get_options(self) -> dict:
        return self._options

    @staticmethod
    def detect_by_name(name: str) -> ParsedFormat:
        content_type = ContentType.detect_by_name(name)
        compress_method = 'gzip' if '.gz' in name else None
        return LeanFormat(content_type=content_type, compress=compress_method).get_defined()

    def get_defined(self, skip_errors: bool = False) -> ParsedFormat:
        props = self.get_props(ex='options')
        props.update(self.get_options())
        content_type = self.get_content_type()
        if content_type == ContentType.TextFile:
            return TextFormat(**props)
        elif content_type == ContentType.JsonFile:
            return JsonFormat(**props)
        elif content_type == ContentType.ColumnFile:
            return ColumnarFormat(**props)
        elif skip_errors:
            return self
        else:
            msg = 'LeanFormat(content_type={}) not supported for LeanFormat.get_defined()'
            raise ValueError(msg.format(self.get_content_type()))

    def set_struct(self, struct: StructInterface, inplace: bool) -> FlatStructFormat:
        if inplace:
            raise ValueError('for ColumnarFormat struct can not be set inplace, use inplace=False instead')
        else:
            return FlatStructFormat(struct=struct, **self.get_props())

    def get_default_stream_type(self) -> StreamType:
        defined_format = self.get_defined()
        if isinstance(defined_format, LeanFormat):
            return StreamType.AnyStream
        else:
            return defined_format.get_default_stream_type()

    def get_default_item_type(self) -> ItemType:
        defined_format = self.get_defined()
        if isinstance(defined_format, LeanFormat):
            return ItemType.Any
        else:
            return defined_format.get_default_item_type()

    def cab_be_stream(self) -> bool:
        return True

    def is_text(self) -> bool:
        return self.get_content_type() in TEXT_TYPES

    def get_formatted_item(self, item: Item, item_type: Union[ItemType, Auto] = AUTO):
        defined_format = self.get_defined(skip_errors=False)
        return defined_format.get_formatted_item(item, item_type=item_type)

    def get_parsed_line(self, line: str, item_type: Union[ItemType, Auto] = AUTO) -> Item:
        defined_format = self.get_defined(skip_errors=False)
        return defined_format.get_parsed_line(line, item_type=item_type)


ContentType.set_dict_classes(
    {
        ContentType.TextFile: TextFormat,
        ContentType.JsonFile: JsonFormat,
        ContentType.ColumnFile: ColumnarFormat,
        ContentType.CsvFile: ColumnarFormat,
        ContentType.TsvFile: ColumnarFormat,
    }
)
