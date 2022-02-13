from typing import Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, Value, AutoCount
    from content.representations.abstract_repr import (
        AbstractRepresentation, ReprType, OptKey,
        DEFAULT_LEN, DEFAULT_STR, CROP_SUFFIX, FILL_CHAR,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import AUTO, Auto, Value, AutoCount
    from .abstract_repr import (
        AbstractRepresentation, ReprType, OptKey,
        DEFAULT_LEN, DEFAULT_STR, CROP_SUFFIX, FILL_CHAR,
    )

DEFAULT_PRECISION = 3


class NumericRepresentation(AbstractRepresentation):
    def __init__(
            self,
            precision: int = DEFAULT_PRECISION,
            use_percent: bool = False,
            use_plus: Union[bool, str] = False,
            allow_exp: bool = False,
            align_right: bool = False,
            min_len: AutoCount = DEFAULT_LEN,
            max_len: AutoCount = AUTO,
            crop: str = CROP_SUFFIX,
            fill: str = FILL_CHAR,
            default: str = DEFAULT_STR,
    ):
        self._precision = precision
        self._use_percent = use_percent
        self._use_plus = use_plus
        self._allow_exp = allow_exp
        super().__init__(
            min_len=min_len, max_len=max_len, fill=fill, crop=crop,
            align_right=align_right, default=default,
        )

    @staticmethod
    def get_repr_type() -> ReprType:
        return ReprType.NumericRepr

    def format(self, value: Value, skip_errors: bool = True) -> str:
        return super().format(value, skip_errors=skip_errors)

    def parse(self, line: str, skip_errors: bool = False) -> Union[int, float, None]:
        value = super().parse(line)
        try:
            if self._precision or '.' in line:
                value = float(value)
            if not self._precision:
                value = int(value)
            return value
        except ValueError as e:
            if skip_errors or line == self._default:
                return None
            else:
                raise ValueError(e)

    def convert_value(self, value: Value) -> Value:
        try:
            if self._precision > 0:
                return float(value)
            else:
                return int(value)
        except ValueError:
            return self._default

    def get_spec_str(self) -> str:
        template = '{fill}{align}{sign}{width}{precision}{type}'
        return template.format(
            fill=self._fill, align=self.get_align_str(), sign=self.get_sign_str(),
            width=self._min_len, precision=self.get_precision_str(),
            type=self.get_type_str(),
        )

    def get_sign_str(self) -> str:
        if isinstance(self._use_plus, str):
            return self._use_plus
        elif self._use_plus:
            return '+'
        else:
            return '-'

    def get_precision_str(self) -> str:
        if self._precision > 0:
            return '.{}'.format(self._precision)
        else:
            return ''

    def get_type_str(self) -> str:
        if self._use_percent:
            return '%'
        elif self._allow_exp:
            return 'g'
        elif self._precision:
            return 'f'
        else:
            return 'd'


ReprType.add_classes(NumericRepr=NumericRepresentation)
