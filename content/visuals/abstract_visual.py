from abc import ABC, abstractmethod
from typing import Tuple, Optional

try:  # Assume we're a submodule in a package.
    from content.visuals.unit_type import UnitType
    from content.visuals.screen_context import ScreenContext, Numeric, NUMERIC_TYPES
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .unit_type import UnitType
    from .screen_context import ScreenContext, Numeric, NUMERIC_TYPES

DEFAULT_UNIT_TYPE = UnitType.get_default()
DEFAULT_SCREEN_CONTEXT = ScreenContext()


class AbstractVisual(ABC):
    def __init__(
            self,
            x,
            unit_type: UnitType = UnitType.Auto,
            screen_context: ScreenContext = ScreenContext.Auto,
    ):
        self._x = x
        self._unit_type = unit_type
        self._screen_context = screen_context

    @classmethod
    def from_numeric(
            cls,
            value: Numeric,
            unit_type: UnitType = UnitType.Auto,
            screen_context: Optional[ScreenContext] = ScreenContext.Auto,
    ):
        assert isinstance(value, NUMERIC_TYPES), value
        return cls(value, unit_type=unit_type, screen_context=screen_context)

    def get_x(self) -> Numeric:
        return self._x

    def get_unit_type(self) -> UnitType:
        if self._unit_type is None or self._unit_type == UnitType.Auto:
            return DEFAULT_UNIT_TYPE
        else:
            return self._unit_type

    def get_screen_context(self) -> ScreenContext:
        if self._screen_context is None or self._screen_context == ScreenContext.Auto:
            return DEFAULT_SCREEN_CONTEXT
        else:
            return self._screen_context

    def set_x(self, value: Numeric, inplace: bool):
        if inplace:
            self._set_x_inplace(value)
            return self
        else:
            return self.__class__(value, unit_type=self._unit_type, screen_context=self._screen_context)

    def set_unit_type(self, unit_type: UnitType, inplace: bool, skip_empty: bool = False):
        if skip_empty and (unit_type is None or unit_type == UnitType.Auto):
            return self
        if inplace:
            self._set_unit_type_inplace(unit_type)
            return self
        else:
            return self.__class__(self.x, unit_type=unit_type, screen_context=self.screen_context)

    def set_screen_context(self, screen_context: ScreenContext, inplace: bool):
        if inplace:
            self._set_screen_context_inplace(screen_context)
            return self
        else:
            return self.__class__(self.x, unit_type=self.unit_type, screen_context=screen_context)

    def _set_x_inplace(self, value: Numeric) -> None:
        self._x = value

    def _set_unit_type_inplace(self, unit_type: UnitType) -> None:
        self._unit_type = unit_type

    def _set_screen_context_inplace(self, screen_context) -> None:
        self._screen_context = screen_context

    x = property(get_x, _set_x_inplace)
    unit_type = property(get_unit_type, _set_unit_type_inplace)
    screen_context = property(get_screen_context, _set_screen_context_inplace)

    def get_first_value(self) -> Numeric:
        return self.get_x()

    def get_value(self) -> Numeric:
        return self.get_x()

    def set_value(self, value: Numeric, inplace: bool):
        return self.set_x(value, inplace=inplace)

    def _set_value_inplace(self, value: Numeric) -> None:
        self._set_x_inplace(value)

    value = property(get_value, _set_value_inplace)

    def get_html_code(self) -> str:
        return str(self)

    def __str__(self):
        return str(self.get_value())


class Abstract2d(AbstractVisual, ABC):
    def get_first_value(self) -> Numeric:
        return self._y

    def get_y(self) -> Numeric:
        return self._y

    def set_y(self, value: Numeric, inplace: bool):
        if inplace:
            self._set_y_inplace(value)
            return self
        else:
            return self.__class__(y=value, x=self.x, unit_type=self._unit_type, screen_context=self._screen_context)

    def _set_y_inplace(self, value: Numeric) -> None:
        self._y = value

    y = property(get_y, _set_y_inplace)

    @abstractmethod
    def get_vertical(self):
        pass

    @abstractmethod
    def get_horizontal(self):
        pass

    vertical = property(get_vertical)
    horizontal = property(get_horizontal)

    def get_vertical_units(self) -> Numeric:
        return self._y

    def get_horizontal_units(self) -> Numeric:
        return self._x

    def get_value(self) -> Tuple[Numeric, Numeric]:
        return self._y, self._x

    def __eq__(self, other):
        vertical_eq = self.vertical.get_px() == other.vertical.get_px()
        horizontal_eq = self.horizontal.get_px() == other.horizontal.get_px()
        return vertical_eq and horizontal_eq
