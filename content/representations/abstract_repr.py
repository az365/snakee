from abc import ABC

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.classes.typing import Value, Count
    from base.abstract.abstract_base import AbstractBaseObject
    from content.representations.repr_interface import RepresentationInterface, ReprType, OptKey
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import Auto, AUTO
    from ...base.classes.typing import Value, Count
    from ...base.abstract.abstract_base import AbstractBaseObject
    from .repr_interface import RepresentationInterface, ReprType, OptKey

Native = RepresentationInterface

DEFAULT_LEN = 7
DEFAULT_STR = '-'
CROP_SUFFIX = '..'
FILL_CHAR = ' '


class AbstractRepresentation(RepresentationInterface, AbstractBaseObject, ABC):
    def __init__(
            self,
            align_right: bool = False,
            min_len: Count = AUTO,
            max_len: Count = AUTO,
            crop: str = CROP_SUFFIX,
            fill: str = FILL_CHAR,
            prefix: str = '',
            suffix: str = '',
            default: str = DEFAULT_STR,
    ):
        max_len = Auto.acquire(max_len, min_len if Auto.is_defined(min_len) else DEFAULT_LEN)
        min_len = Auto.acquire(min_len, max_len)
        assert len(crop) <= max_len, 'Expected len(crop) <= max_len, got len({}) > {}'.format(crop, max_len)
        self._align_right = align_right
        self._min_len = min_len
        self._max_len = max_len
        self._crop = crop
        self._fill = fill
        self._prefix = prefix
        self._suffix = suffix
        self._default = default

    @classmethod
    def get_type(cls) -> ReprType:
        return cls.get_repr_type()

    def set_count(self, value: int, inplace: bool) -> Native:
        if inplace:
            self._min_len = value
            self._max_len = value
            return self
        else:
            representation = self.set_outplace(min_len=value, max_len=value)
            return self._assume_native(representation)

    def convert_value(self, value: Value) -> Value:
        if value is None:
            value = self._default
        return value

    def get_cropped(self, line: str) -> str:
        if Auto.is_defined(self._max_len):
            if 0 < self._max_len < len(line):
                line = line[:self._max_len - len(self._crop)] + self._crop
        return line

    def format(self, value: Value, skip_errors: bool = False) -> str:
        value = self.convert_value(value)
        try:
            line = self.get_template().format(value)
        except ValueError as e:
            if skip_errors:
                line = self.get_default_template().format(value)
            else:
                raise ValueError(e)
        line = self.get_cropped(line)
        return line

    def parse(self, line: str) -> Value:
        return line.strip(self._fill)

    def get_template(self, key: OptKey = None) -> str:
        return self.get_default_template(key=key)

    def get_default_template(self, key: OptKey = None) -> str:
        template = '{start}{key}:{spec}{end}'
        return template.format(start='{', key=str(key or ''), spec=self.get_default_spec_str(), end='}')

    def get_spec_str(self) -> str:
        return self.get_default_spec_str()

    def get_default_spec_str(self) -> str:
        template = '{fill}{align}{width}'
        return template.format(fill=self._fill, align=self.get_align_str(), width=self._min_len)

    def get_align_str(self) -> str:
        if self._align_right:
            return '>'
        else:
            return '<'

    @staticmethod
    def _assume_native(representation):
        return representation

    def __repr__(self):
        return "'" + self.get_template() + "'"
