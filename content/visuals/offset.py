from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from content.visuals.unit_type import UnitType
    from content.visuals.screen_context import ScreenContext, Numeric, NUMERIC_TYPES
    from content.visuals.abstract_visual import AbstractVisual, DEFAULT_UNIT_TYPE, DEFAULT_SCREEN_CONTEXT
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .unit_type import UnitType
    from .screen_context import ScreenContext, Numeric, NUMERIC_TYPES
    from .abstract_visual import AbstractVisual, DEFAULT_UNIT_TYPE, DEFAULT_SCREEN_CONTEXT

Value = Union[Numeric, str]


class Offset(AbstractVisual):
    def __init__(
            self,
            x: Value,
            unit_type: UnitType = UnitType.Auto,
            screen_context: ScreenContext = ScreenContext.Auto,
    ):
        super().__init__(0, unit_type=unit_type, screen_context=screen_context)
        self.set_x(value=x, inplace=True)

    @classmethod
    def from_str(
            cls,
            value: str,
            expected_unit_type: UnitType = UnitType.Auto,
            screen_context: ScreenContext = ScreenContext.Auto,
    ):
        x, unit_type = UnitType.parse(value, expected_unit_type=expected_unit_type)
        return Offset(x, unit_type=unit_type, screen_context=screen_context)

    @classmethod
    def from_visual(
            cls,
            value: AbstractVisual,
            expected_unit_type: UnitType = UnitType.Auto,
            screen_context: ScreenContext = ScreenContext.Auto,
    ):
        x = value.x
        if hasattr(x, 'unit_type'):
            unit_type = value.unit_type
            if expected_unit_type:
                assert unit_type == expected_unit_type
        else:
            unit_type = expected_unit_type
        if hasattr(x, 'screen_context'):
            screen_context = value.screen_context
        return Offset(x, unit_type=unit_type, screen_context=screen_context)

    @classmethod
    def from_any(
            cls,
            value,
            expected_unit_type: UnitType = UnitType.Auto,
            screen_context: ScreenContext = ScreenContext.Auto,
    ):
        if isinstance(value, Offset):
            return value
        elif isinstance(value, NUMERIC_TYPES):
            return Offset(value, unit_type=expected_unit_type, screen_context=screen_context)
        elif isinstance(value, str):
            return Offset.from_str(value, expected_unit_type=expected_unit_type, screen_context=screen_context)
        elif isinstance(value, (tuple, list)):
            return Offset(*value)
        elif isinstance(value, AbstractVisual) or hasattr(value, 'get_x'):
            return Offset.from_visual(value, expected_unit_type=expected_unit_type, screen_context=screen_context)
        elif hasattr(value, 'get_value'):
            return Offset(value.get_value, unit_type=expected_unit_type, screen_context=screen_context)
        else:
            raise TypeError(value)

    def _set_x_inplace(self, value: Value) -> None:
        if isinstance(value, NUMERIC_TYPES):
            super()._set_x_inplace(value)
        elif isinstance(value, str):
            x, unit_type = UnitType.parse(value)
            self._x = x
            self._unit_type = unit_type
        elif isinstance(value, AbstractVisual) or hasattr(value, 'get_first_value'):
            self._x = value.get_first_value()
            self._unit_type = value.get_unit_type()
        else:
            raise TypeError(value)

    def get_value(self) -> Value:
        if self.unit_type is None or self.unit_type == UnitType.Auto:
            return self.get_x()
        else:
            return f'{self.x}{self.unit_type.value}'
        # return str(self)

    def set_value(self, value: Value, inplace: bool):
        if inplace:
            self._set_value_inplace(value)
            return self
        else:
            return self.from_any(value, expected_unit_type=self.unit_type, screen_context=self.screen_context)

    def _set_value_inplace(self, value: Value) -> None:
        if isinstance(value, NUMERIC_TYPES):
            self.set_x(value, inplace=True)
        elif isinstance(value, str):
            x, unit_type = UnitType.parse(value)
            self._x = x
            self._unit_type = unit_type
        elif isinstance(value, AbstractVisual) or hasattr(value, 'get_first_value'):
            self._x = value.get_first_value()
            self._unit_type = value.get_unit_type()
        else:
            raise TypeError(value)

    def get_unit_type(self) -> UnitType:
        return super().get_unit_type()

    def _set_unit_type_inplace(self, unit_type: UnitType) -> None:
        if self._unit_type is not None and self._unit_type != UnitType.Auto:
            self._x = UnitType.convert(self._x, src=self._unit_type, dst=unit_type, screen_context=self.screen_context)
        self._unit_type = unit_type

    unit_type = property(get_unit_type, _set_unit_type_inplace)

    def get_px(self, screen_context: Optional[ScreenContext] = ScreenContext.Auto) -> int:
        if screen_context is None or screen_context == ScreenContext.Auto:
            screen_context = self.get_screen_context()
        px = UnitType.convert(self.x, src=self.unit_type, dst=UnitType.Pixel, screen_context=screen_context)
        return int(px)

    def get_em(self, screen_context: Optional[ScreenContext] = ScreenContext.Auto) -> float:
        if screen_context is None or screen_context == ScreenContext.Auto:
            screen_context = self.get_screen_context()
        em = UnitType.convert(self.x, src=self.unit_type, dst=UnitType.Font, screen_context=screen_context)
        return round(em, 1)

    def get_symbols_count(self, screen_context: Optional[ScreenContext] = ScreenContext.Auto) -> float:
        if screen_context is None or screen_context == ScreenContext.Auto:
            screen_context = self.get_screen_context()
        count = UnitType.convert(self.x, src=self.unit_type, dst=UnitType.Symbol, screen_context=screen_context)
        return round(count, 1)

    def get_width_share(self, screen_context: Optional[ScreenContext] = ScreenContext.Auto) -> float:
        if screen_context is None or screen_context == ScreenContext.Auto:
            screen_context = self.get_screen_context()
        share = UnitType.convert(self.x, src=self.unit_type, dst=UnitType.Width, screen_context=screen_context)
        return round(share, 3)

    def get_percent(self, screen_context: Optional[ScreenContext] = ScreenContext.Auto) -> float:
        if screen_context is None or screen_context == ScreenContext.Auto:
            screen_context = self.get_screen_context()
        percent = UnitType.convert(self.x, src=self.unit_type, dst=UnitType.Percent, screen_context=screen_context)
        return int(percent)

    def get_units(self, unit_type: UnitType, screen_context: Optional[ScreenContext] = ScreenContext.Auto) -> float:
        if screen_context is None or screen_context == ScreenContext.Auto:
            screen_context = self.get_screen_context()
        return UnitType.convert(self.x, src=self.unit_type, dst=unit_type, screen_context=screen_context)

    def get_lines_count(self) -> int:
        return int(self.get_em())

    def get_line_len(self) -> int:
        return int(self.get_symbols_count())

    def get_html_code(self) -> str:
        if self.unit_type.supports_html():
            html_code = str(self)
        elif self.unit_type == UnitType.Width:
            percent = int(self.get_percent())
            html_code = f'{percent}%'
        else:
            pixels = self.get_px()
            html_code = f'{pixels}px'
        return html_code

    def __str__(self):
        return str(self.get_value())

    def __eq__(self, other):
        return self.get_px() == other.get_px()
