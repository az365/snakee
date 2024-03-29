try:  # Assume we're a submodule in a package.
    from base.constants.chars import (
        EMPTY, CROP_SUFFIX, FILL_CHAR, DEFAULT_STR,
        TAB_CHAR, TAB_SUBSTITUTE, PARAGRAPH_CHAR, PARAGRAPH_SUBSTITUTE,
    )
    from content.representations.abstract_repr import AbstractRepresentation, ReprType, OptKey, Value, Count
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import (
        EMPTY, CROP_SUFFIX, FILL_CHAR, DEFAULT_STR,
        TAB_CHAR, TAB_SUBSTITUTE, PARAGRAPH_CHAR, PARAGRAPH_SUBSTITUTE,
    )
    from .abstract_repr import AbstractRepresentation, ReprType, OptKey, Value, Count


class StringRepresentation(AbstractRepresentation):
    def __init__(
            self,
            align_right: bool = False,
            min_len: Count = None,
            max_len: Count = None,
            including_framing: bool = False,
            crop: str = CROP_SUFFIX,  # '..'
            fill: str = FILL_CHAR,  # ' '
            tab: str = TAB_SUBSTITUTE,  # ' -> '
            paragraph: str = PARAGRAPH_SUBSTITUTE,  # ' \\n '
            prefix: str = EMPTY,  # ''
            suffix: str = EMPTY,  # ''
            default: str = DEFAULT_STR,  # '-'
    ):
        self._tab = tab
        self._paragraph = paragraph
        super().__init__(
            min_len=min_len, max_len=max_len, including_framing=including_framing,
            fill=fill, crop=crop,
            prefix=prefix, suffix=suffix,
            align_right=align_right, default=default,
        )

    @staticmethod
    def get_repr_type() -> ReprType:
        return ReprType.StringRepr

    def format(self, value: Value, key: OptKey = None, skip_errors: bool = False) -> str:
        if value is None:
            value = self.get_default()
        if not isinstance(value, str):
            value = str(value)
        if self._tab is not None:
            value = value.replace(TAB_CHAR, self._tab)
        if self._paragraph is not None:
            value = value.replace(PARAGRAPH_CHAR, self._paragraph)
        return super().format(value)

    def parse(self, line: str) -> str:
        value = super().parse(line)
        if self._tab is not None:
            value = value.replace(self._tab, TAB_CHAR)
        if self._paragraph is not None:
            value = value.replace(self._paragraph, PARAGRAPH_CHAR)
        return value


ReprType.add_classes(StringRepr=StringRepresentation)
