from abc import ABC

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.classes.typing import Value, Count
    from base.constants.chars import EMPTY, FILL_CHAR, DEFAULT_STR, CROP_SUFFIX
    from base.abstract.abstract_base import AbstractBaseObject
    from content.representations.repr_interface import RepresentationInterface, ReprType, OptKey
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import Auto, AUTO
    from ...base.classes.typing import Value, Count
    from ...base.constants.chars import EMPTY, FILL_CHAR, DEFAULT_STR, CROP_SUFFIX
    from ...base.abstract.abstract_base import AbstractBaseObject
    from .repr_interface import RepresentationInterface, ReprType, OptKey

Native = RepresentationInterface


class AbstractRepresentation(AbstractBaseObject, RepresentationInterface, ABC):
    def __init__(
            self,
            align_right: bool = False,
            min_len: Count = AUTO,
            max_len: Count = AUTO,
            including_framing: bool = False,
            crop: str = CROP_SUFFIX,
            fill: str = FILL_CHAR,
            prefix: str = EMPTY,
            suffix: str = EMPTY,
            default: str = DEFAULT_STR,
    ):
        if Auto.is_defined(max_len):
            assert len(crop) <= max_len, 'Expected len(crop) <= max_len, got len({}) > {}'.format(crop, max_len)
        self._min_len = min_len
        self._max_len = max_len
        self._crop = crop
        self._fill = fill
        self._align_right = align_right
        self._prefix = prefix
        self._suffix = suffix
        self._default = default
        if including_framing:
            framing_len = self.get_framing_len()
            if Auto.is_defined(max_len):
                assert max_len >= framing_len, 'Expected max_len >= framing_len, got {}<{}'.format(max_len, framing_len)
                self._max_len -= framing_len
            if Auto.is_defined(min_len):
                if min_len > framing_len:
                    self._min_len -= framing_len
                else:
                    self._min_len = 0

    @classmethod
    def get_type(cls) -> ReprType:
        return cls.get_repr_type()

    def get_fill_char(self):
        return self._fill

    def get_min_value_len(self, or_max: bool = True) -> Count:
        min_value_len = self._min_len
        if Auto.is_auto(min_value_len):
            return self.get_max_value_len(or_min=False) if or_max else None
        else:
            return min_value_len

    def get_max_value_len(self, or_min: bool = True) -> Count:
        max_value_len = self._max_len
        if Auto.is_auto(max_value_len):
            return self.get_min_value_len(or_max=False) if or_min else None
        else:
            return max_value_len

    def get_min_total_len(self, or_max: bool = True) -> Count:
        min_value_len = self.get_min_value_len(or_max=or_max)
        if min_value_len:
            return min_value_len + self.get_framing_len()
        else:
            return 0

    def get_max_total_len(self, or_min: bool = True) -> Count:
        max_value_len = self.get_max_value_len(or_min=or_min)
        if max_value_len is None:
            return None
        else:
            return max_value_len + self.get_framing_len()

    def get_framing_len(self) -> int:
        return len(self._prefix) + len(self._suffix)

    def get_count(self, get_min: bool = False) -> Count:
        if get_min:
            return self.get_min_total_len()
        else:
            return self.get_max_total_len()

    def set_count(self, value: int, inplace: bool) -> Native:
        if inplace:
            self._min_len = value
            self._max_len = value
            return self
        else:
            representation = self.set_outplace(min_len=value, max_len=value)
            return self._assume_native(representation)

    def get_default(self) -> str:
        return self._default

    def convert_value(self, value: Value) -> Value:
        if value is None:
            value = self._default
        return value

    def get_crop_str(self) -> str:
        return self._crop

    def get_cropped(self, line: str) -> str:
        max_len = self.get_max_total_len()
        if Auto.is_defined(max_len):
            if max_len < len(line):
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
                try:
                    line = self.get_default_template().format(value)
                except ValueError:
                    line = str(value)
            else:
                template = '{obj}.format({value}): {e}'
                msg = template.format(obj=repr(self), value=repr(value), e=e)
                raise ValueError(msg)
        line = self.get_cropped(line)
        return line

    def parse(self, line: str) -> Value:
        return line.strip(self.get_fill_char())

    def get_template(self, key: OptKey = None) -> str:
        return self.get_default_template(key=key)

    def get_default_template(self, key: OptKey = None) -> str:
        template = '{prefix}{start}{key}{spec}{end}{suffix}'
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
        width = self.get_min_value_len()
        if width:
            template = ':{fill}{align}{width}'
            return template.format(fill=self.get_fill_char(), align=self.get_align_str(), width=width)
        else:
            return EMPTY

    def get_align_str(self) -> str:
        if self._align_right:
            return '>'
        else:
            return '<'

    @staticmethod
    def _assume_native(representation) -> Native:
        return representation

    def __repr__(self):
        return repr(self.get_template())
