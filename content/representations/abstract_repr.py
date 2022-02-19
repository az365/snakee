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

    def get_max_len(self, or_min: bool = True) -> Count:
        if Auto.is_auto(self._max_len):
            if or_min:
                return self.get_min_len(or_max=False)
            else:
                return None
        else:
            return self._max_len

    def get_min_len(self, or_max: bool = True) -> Count:
        if Auto.is_auto(self._min_len):
            if or_max:
                return self.get_max_len(or_min=False)
            else:
                return None
        else:
            return self._min_len

    def get_max_value_len(self) -> Count:
        max_total_len = self.get_max_len()
        if max_total_len is None:
            return None
        else:
            max_value_len = max_total_len - len(self._prefix) - len(self._suffix)
            if max_value_len > 0:
                return max_value_len
            else:
                return 0

    def get_min_value_len(self) -> Count:
        min_total_len = self.get_min_len()
        if min_total_len:
            min_value_len = min_total_len - len(self._prefix) - len(self._suffix)
            if min_value_len > 0:
                return min_value_len
        return 0

    def get_count(self, get_min: bool = False) -> Count:
        if get_min:
            return self.get_min_len()
        else:
            return self.get_max_len()

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

    def get_crop_str(self) -> str:
        return self._crop

    def get_cropped(self, line: str) -> str:
        max_len = self.get_max_len()
        if Auto.is_defined(max_len):
            if 0 < max_len < len(line):
                crop_str = self.get_crop_str()
                crop_len = max_len - len(crop_str)
                if crop_len > 0:
                    line = line[:crop_len] + crop_str
                else:
                    line = crop_str[:-crop_len]
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
        template = '{prefix}{start}{key}:{spec}{end}{suffix}'
        return template.format(
            prefix=self._prefix,
            start='{',
            key=str(key or ''),
            spec=self.get_default_spec_str(),
            end='}',
            suffix=self._suffix,
        )

    def get_spec_str(self) -> str:
        return self.get_default_spec_str()

    def get_default_spec_str(self) -> str:
        template = '{fill}{align}{width}'
        return template.format(fill=self._fill, align=self.get_align_str(), width=self.get_min_value_len())

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
