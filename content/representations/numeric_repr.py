from typing import Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, Value, AutoCount
    from content.representations.repr_constants import DEFAULT_STR, CROP_SUFFIX, FILL_CHAR, DEFAULT_PRECISION
    from content.representations.abstract_repr import AbstractRepresentation, ReprType, OptKey
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import AUTO, Auto, Value, AutoCount
    from .repr_constants import DEFAULT_STR, CROP_SUFFIX, FILL_CHAR, DEFAULT_PRECISION
    from .abstract_repr import AbstractRepresentation, ReprType, OptKey


class NumericRepresentation(AbstractRepresentation):
    def __init__(
            self,
            precision: int = DEFAULT_PRECISION,
            use_percent: bool = False,
            use_plus: Union[bool, str] = False,
            allow_exp: bool = False,
            align_right: bool = True,
            min_len: AutoCount = AUTO,
            max_len: AutoCount = AUTO,
            including_framing: bool = False,
            crop: str = CROP_SUFFIX,
            fill: str = FILL_CHAR,
            prefix: str = '',
            suffix: str = '',
            default: str = DEFAULT_STR,
    ):
        self._precision = precision
        self._use_percent = use_percent
        self._use_plus = use_plus
        self._allow_exp = allow_exp
        super().__init__(
            min_len=min_len, max_len=max_len, including_framing=including_framing,
            fill=fill, crop=crop,
            prefix=prefix, suffix=suffix,
            align_right=align_right, default=default,
        )

    @staticmethod
    def get_repr_type() -> ReprType:
        return ReprType.NumericRepr

    def get_precision(self):
        return self._precision

    def format(self, value: Value, skip_errors: bool = True) -> str:
        return super().format(value, skip_errors=skip_errors)

    def parse(self, line: str, skip_errors: bool = False) -> Union[int, float, None]:
        value = super().parse(line)
        try:
            if self.get_precision():
                if '.' in line:
                    value = float(value)
            else:
                value = int(value)
            return value
        except ValueError as e:
            if skip_errors or line == self.get_default():
                return None
            else:
                raise ValueError(e)

    def convert_value(self, value: Value) -> Value:
        try:
            if self.get_precision() > 0:
                return float(value)
            else:
                return int(value)
        except (ValueError, TypeError):
            return self.get_default()

    def get_template(self, key: OptKey = None) -> str:
        template = '{prefix}{start}{key}{spec}{end}{suffix}'
        return template.format(
            prefix=self._prefix,
            start='{',
            key=str(key or ''),
            spec=self.get_spec_str(),
            end='}',
            suffix=self._suffix,
        )

    def get_spec_str(self) -> str:
        width = self.get_min_value_len()
        sign = self.get_sign_str()
        precision = self.get_precision_str()
        type_str = self.get_type_str()
        if width or sign or precision is not None or type_str != 'd':
            template = ':{fill}{align}{sign}{width}{precision}{type}'
            return template.format(
                fill=self._fill, align=self.get_align_str(), sign=sign,
                width=width, precision=precision, type=type_str,
            )
        else:
            return ''

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
