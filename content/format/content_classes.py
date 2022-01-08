try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from content.format.abstract_format import AbstractFormat, BinaryFormat, CompressibleFormat, ParsedFormat
    from content.format.text_format import TextFormat, JsonFormat, DEFAULT_ENDING, DEFAULT_ENCODING
    from content.format.columnar_format import ColumnarFormat, FlatStructFormat
    from content.format.lean_format import LeanFormat
    from content.format.content_type import ContentType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated_with_alternative
    from .abstract_format import AbstractFormat, BinaryFormat, CompressibleFormat, ParsedFormat
    from .text_format import TextFormat, JsonFormat, DEFAULT_ENDING, DEFAULT_ENCODING
    from .columnar_format import ColumnarFormat, FlatStructFormat
    from .lean_format import LeanFormat
    from .content_type import ContentType

DEFAULT_DELIMITER = ','


class CsvFormat(FlatStructFormat):
    @deprecated_with_alternative('FlatStructFormat.__init__()')
    def __init__(
            self,
            struct=arg.AUTO,
            first_line_is_title: bool = True,
            delimiter: str = DEFAULT_DELIMITER,
            ending: str = DEFAULT_ENDING,
            encoding: str = DEFAULT_ENCODING,
            compress=None,
    ):
        super().__init__(
            struct=struct,
            delimiter=delimiter, first_line_is_title=first_line_is_title,
            ending=ending, encoding=encoding, compress=compress,
        )


ContentType.set_dict_classes(
    {
        ContentType.TextFile: TextFormat,
        ContentType.JsonFile: JsonFormat,
        ContentType.ColumnFile: ColumnarFormat,
        ContentType.CsvFile: CsvFormat,
        ContentType.TsvFile: FlatStructFormat,
    }
)
