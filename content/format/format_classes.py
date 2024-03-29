try:  # Assume we're a submodule in a package.
    from base.constants.chars import COMMA, PARAGRAPH_CHAR
    from base.constants.text import DEFAULT_ENCODING
    from utils.decorators import deprecated_with_alternative
    from content.format.abstract_format import AbstractFormat, BinaryFormat, CompressibleFormat, ParsedFormat
    from content.format.text_format import TextFormat, JsonFormat
    from content.format.document_format import DocumentFormat, MarkdownFormat, HtmlFormat
    from content.format.columnar_format import ColumnarFormat, FlatStructFormat
    from content.format.lean_format import LeanFormat
    from content.format.content_type import ContentType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import COMMA, PARAGRAPH_CHAR
    from ...base.constants.text import DEFAULT_ENCODING
    from ...utils.decorators import deprecated_with_alternative
    from .abstract_format import AbstractFormat, BinaryFormat, CompressibleFormat, ParsedFormat
    from .text_format import TextFormat, JsonFormat
    from .document_format import DocumentFormat, MarkdownFormat, HtmlFormat
    from .columnar_format import ColumnarFormat, FlatStructFormat
    from .lean_format import LeanFormat
    from .content_type import ContentType


class CsvFormat(FlatStructFormat):
    @deprecated_with_alternative('FlatStructFormat.__init__()')
    def __init__(
            self,
            struct=None,
            first_line_is_title: bool = True,
            delimiter: str = COMMA,  # ','
            ending: str = PARAGRAPH_CHAR,  # '\n'
            encoding: str = DEFAULT_ENCODING,  # 'utf8'
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
        ContentType.Markdown: MarkdownFormat,
        ContentType.Html: HtmlFormat,
    }
)
