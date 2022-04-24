from typing import Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, Value, AutoCount
    from base.constants.chars import FILL_CHAR, DEFAULT_STR, CROP_SUFFIX
    from functions.secondary.cast_functions import number
    from content.representations.abstract_repr import AbstractRepresentation, ReprType, OptKey
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import AUTO, Auto, Value, AutoCount
    from ...base.constants.chars import FILL_CHAR, DEFAULT_STR, CROP_SUFFIX
    from ...functions.secondary.cast_functions import number
    from .abstract_repr import AbstractRepresentation, ReprType, OptKey

DEFAULT_PRECISION = 3


class NumericRepresentation(AbstractRepresentation):
    def __init__(
            self,
            precision: int = DEFAULT_PRECISION,
            use_percent: bool = False,
            use_plus: Union[bool, str] = False,
            use_letter: bool = False,
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
        self._use_letter = use_letter
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

    def using_percent(self) -> bool:
        return self._use_percent

    def using_plus(self) -> Union[bool, str]:
        return self._use_plus

    def using_letter(self) -> bool:
        return self._use_letter

    def get_precision(self) -> int:
        return self._precision

    def format(self, value: Value, skip_errors: bool = True) -> str:
        return super().format(value, skip_errors=skip_errors)

    def parse(self, line: str, skip_errors: bool = False) -> Union[int, float, None]:
        value = super().parse(line)
        if value:
            if self.using_letter():
                raise NotImplementedError
            try:
                if value[-1] == '%':  # or self.using_percent()
                    value = self.parse(value[:-1], skip_errors=skip_errors) / 100
                else:
                    if '.' in value:
                        value = float(value)
                    if not self.get_precision():
                        value = int(value)
                return value
            except ValueError as e:
                if skip_errors or value == self.get_default() or line == self.get_default():
                    return None
                else:
                    raise ValueError(e)

    def convert_value(self, value: Value) -> Value:
        int_precision = -2 if self.using_percent() else 0
        try:
            if self.using_letter():
                cast_func = number(
                    str, round_digits=self.get_precision(),
                    show_plus=self.using_plus(), default_value=self.get_default(),
                )
                return cast_func(value)
            elif self.get_precision() > int_precision:
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
            if self.using_letter():  # isinstance(value, str)
                template = ':{}{align}{width}'
            else:  # isinstance(value, float)
                template = ':{fill}{align}{sign}{width}{precision}{type}'
            return template.format(
                fill=self.get_fill_char(), align=self.get_align_str(), sign=sign,
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
        if self.get_precision() >= 0:
            return '.{}'.format(self.get_precision())
        else:
            return ''

    def get_type_str(self) -> str:
        if self.using_percent():
            return '%'
        elif self._allow_exp:
            return 'g'
        elif self.get_precision():
            return 'f'
        else:
            return 'd'


ReprType.add_classes(NumericRepr=NumericRepresentation)
