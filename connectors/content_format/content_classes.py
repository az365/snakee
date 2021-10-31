try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from connectors.content_format.abstract_format import AbstractFormat, BinaryFormat, CompressibleFormat, ParsedFormat
    from connectors.content_format.text_format import TextFormat, JsonFormat
    from connectors.content_format.columnar_format import ColumnarFormat, FlatStructFormat
    from connectors.content_format.lean_format import LeanFormat
    from connectors.content_format.content_type import ContentType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated_with_alternative
    from .abstract_format import AbstractFormat, BinaryFormat, CompressibleFormat, ParsedFormat
    from .text_format import TextFormat, JsonFormat
    from .columnar_format import ColumnarFormat, FlatStructFormat
    from .lean_format import LeanFormat
    from .content_type import ContentType


class CsvFormat(FlatStructFormat):
    @deprecated_with_alternative('FlatStructFormat.__init__()')
    def __init__(
            self,
            first_line_is_title: bool = True,
            delimiter: str = ',',
            ending: str = '\n',
            encoding: str = 'utf8',
            compress=None,
    ):
        super().__init__(
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
