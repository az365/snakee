from typing import Union, Iterable

try:  # Assume we're a submodule in a package.
    from base.constants.chars import EMPTY, FILL_CHAR, DEFAULT_ITEMS_DELIMITER, DEFAULT_STR, CROP_SUFFIX
    from content.representations.abstract_repr import AbstractRepresentation, ReprType, OptKey, Count
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import EMPTY, FILL_CHAR, DEFAULT_ITEMS_DELIMITER, DEFAULT_STR, CROP_SUFFIX
    from .abstract_repr import AbstractRepresentation, ReprType, OptKey, Count

DEFAULT_ITEM_LEN = 23


class SequenceRepresentation(AbstractRepresentation):
    def __init__(
            self,
            delimiter: str = DEFAULT_ITEMS_DELIMITER,  # ', '
            item_representation: Union[AbstractRepresentation, str, None] = None,
            align_right: bool = False,
            min_len: int = DEFAULT_ITEM_LEN,
            max_len: Count = None,
            including_framing: bool = False,
            crop: str = CROP_SUFFIX,  # '..'
            fill: str = FILL_CHAR,  # ' '
            prefix: str = EMPTY,  # ''
            suffix: str = EMPTY,  # ''
            default: str = DEFAULT_STR,  # '-'
    ):
        self._delimiter = delimiter
        self._item_representation = item_representation
        super().__init__(
            min_len=min_len, max_len=max_len, including_framing=including_framing,
            fill=fill, crop=crop,
            prefix=prefix, suffix=suffix,
            align_right=align_right, default=default,
        )

    @staticmethod
    def get_repr_type() -> ReprType:
        return ReprType.StringRepr

    def convert_value(self, value: Iterable) -> str:
        item_repr = self._item_representation
        if isinstance(item_repr, str):
            if '{' not in item_repr:
                if ':' not in item_repr:
                    item_repr = ':' + item_repr
                item_repr = '{' + item_repr + '}'
        if item_repr:
            items = map(lambda i: item_repr.format(i), value)
        else:
            items = map(str, value)
        line = self._delimiter.join(items)
        return line


ReprType.add_classes(SequenceRepr=SequenceRepresentation)
