try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, Value, AutoCount
    from content.representations.abstract_repr import (
        AbstractRepresentation, ReprType, OptKey,
        DEFAULT_STR, CROP_SUFFIX, FILL_CHAR,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import AUTO, Auto, Value, AutoCount
    from .abstract_repr import (
        AbstractRepresentation, ReprType, OptKey,
        DEFAULT_STR, CROP_SUFFIX, FILL_CHAR,
    )

TAB_SYMBOL, TAB_SUBSTITUTE = '\t', ' -> '
PARAGRAPH_SYMBOL, PARAGRAPH_SUBSTITUTE = '\t', ' \\n '


class StringRepresentation(AbstractRepresentation):
    def __init__(
            self,
            align_right: bool = False,
            min_len: AutoCount = AUTO,
            max_len: AutoCount = AUTO,
            including_framing: bool = False,
            crop: str = CROP_SUFFIX,
            fill: str = FILL_CHAR,
            tab: str = TAB_SUBSTITUTE,
            paragraph: str = PARAGRAPH_SUBSTITUTE,
            prefix: str = '',
            suffix: str = '',
            default: str = DEFAULT_STR,
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
            value = value.replace(TAB_SYMBOL, self._tab)
        if self._paragraph is not None:
            value = value.replace(PARAGRAPH_SYMBOL, self._paragraph)
        return super().format(value)

    def parse(self, line: str) -> str:
        value = super().parse(line)
        if self._tab is not None:
            value = value.replace(self._tab, TAB_SYMBOL)
        if self._paragraph is not None:
            value = value.replace(self._paragraph, PARAGRAPH_SYMBOL)
        return value


ReprType.add_classes(StringRepr=StringRepresentation)
