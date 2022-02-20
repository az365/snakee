from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from content.representations.abstract_repr import (
        AbstractRepresentation, ReprType, Value,
        DEFAULT_STR, FILL_CHAR, DEFAULT_LEN,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import Auto, AUTO
    from .abstract_repr import (
        AbstractRepresentation, ReprType, Value,
        DEFAULT_STR, FILL_CHAR, DEFAULT_LEN,
    )

DEFAULT_TRUE_STR = 'Yes'
DEFAULT_FALSE_STR = 'No'
FALSE_VALUES = 'false', 'no', '-', '0', '0.0', ''
CROP_SUFFIX = '_'


class BooleanRepresentation(AbstractRepresentation):
    def __init__(
            self,
            true: str = DEFAULT_TRUE_STR,
            false: str = DEFAULT_FALSE_STR,
            align_right: bool = False,
            min_len: Union[int, Auto] = AUTO,
            max_len: Union[int, Auto] = AUTO,
            crop: str = CROP_SUFFIX,
            fill: str = FILL_CHAR,
            prefix: str = '',
            suffix: str = '',
            default: str = DEFAULT_STR,
    ):
        max_len = Auto.acquire(max_len, max(len(true), len(false), len(default), len(crop), Auto.acquire(min_len, 0)))
        min_len = Auto.acquire(min_len, max_len)
        self._true = true
        self._false = false
        super().__init__(
            min_len=min_len, max_len=max_len, fill=fill, crop=crop,
            prefix=prefix, suffix=suffix,
            align_right=align_right, default=default,
        )

    @staticmethod
    def get_repr_type() -> ReprType:
        return ReprType.BooleanRepr

    def convert_value(self, value: Optional[bool]) -> Value:
        if value is None:
            return self._default
        elif value:
            return self._true
        else:
            return self._false

    def parse(self, line: str) -> Optional[bool]:
        if line == self._true:
            return True
        elif line == self._false:
            return False
        elif line == self._default:
            return None
        elif line.lower() in FALSE_VALUES:
            return False
        else:
            return True


ReprType.add_classes(BooleanRepr=BooleanRepresentation)
