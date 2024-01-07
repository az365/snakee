from typing import Optional, Tuple, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import NUMERIC_TYPES, ARRAY_TYPES, Numeric, Array
    from content.visuals.unit_type import UnitType
    from content.visuals.screen_context import ScreenContext, DEFAULT_LINE_LEN
    from content.visuals.abstract_visual import AbstractVisual, Abstract2d
    from content.visuals.offset import Offset
    from content.visuals.point import Point
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import NUMERIC_TYPES, ARRAY_TYPES, Numeric, Array
    from .unit_type import UnitType
    from .screen_context import ScreenContext, DEFAULT_LINE_LEN
    from .abstract_visual import AbstractVisual, Abstract2d
    from .offset import Offset
    from .point import Point

Native = Abstract2d

DEFAULT_VALUE = None


class Size(Abstract2d):
    def __init__(
            self,
            vertical: Union[Offset, Numeric, None],
            horizontal: Union[Offset, Numeric, None],
    ):
        self._y = DEFAULT_VALUE
        super().__init__(x=DEFAULT_VALUE)
        self.set_vertical(vertical, inplace=True)
        self.set_horizontal(horizontal, inplace=True)

    @classmethod
    def from_tuple(cls, point_tuple) -> Native:
        point = Point(*point_tuple)
        return Size.from_point(point)

    @classmethod
    def from_point(cls, point) -> Native:
        vertical = Offset(point.y, unit_type=point.unit_type, screen_context=point.screen_context)
        horizontal = Offset(point.x, unit_type=point.unit_type, screen_context=point.screen_context)
        return Size(vertical, horizontal)

    @classmethod
    def from_visual(cls, value: AbstractVisual) -> Native:
        if hasattr(value, 'y'):
            y = value.y
            x = value.x
        else:
            y = value.x
            x = None
        unit_type = value.unit_type
        screen_context = value.screen_context
        if y is None:
            vertical = None
        else:
            vertical = Offset(y, unit_type=unit_type, screen_context=screen_context)
        if x is None:
            horizontal = None
        else:
            horizontal = Offset(x, unit_type=unit_type, screen_context=screen_context)
        return Size(vertical, horizontal)

    @classmethod
    def from_any(cls, value) -> Native:
        if isinstance(value, Size):
            size = value
        elif isinstance(value, Point):
            size = Size.from_point(value)
        elif isinstance(value, Offset):
            size = Size(value, None)
        elif isinstance(value, Abstract2d) or hasattr(value, 'y'):
            size = Size(value.vertical, value.horizontal)
        elif isinstance(value, AbstractVisual) or hasattr(value, 'x'):
            size = Size.from_visual(value)
        elif isinstance(value, ARRAY_TYPES):  # list, tuple
            size = Size.from_tuple(value)
        elif isinstance(value, NUMERIC_TYPES):  # int, float
            size = Size(Offset(value), None)
        else:
            raise TypeError
        return size

    def get_vertical(self) -> Optional[Offset]:
        return self._y

    def get_horizontal(self) -> Optional[Offset]:
        return self._x

    def set_vertical(self, offset: Optional[Offset], inplace: bool = True) -> Native:
        if inplace:
            self._set_vertical_inplace(offset)
            return self
        else:
            return self.__class__(vertical=offset, horizontal=self.get_horizontal())

    def set_horizontal(self, offset: Optional[Offset], inplace: bool = True) -> Native:
        if inplace:
            self._set_horizontal_inplace(offset)
            return self
        else:
            return self.__class__(vertical=self.get_vertical(), horizontal=offset)

    def _set_vertical_inplace(self, offset: Union[Offset, Numeric, None]) -> None:
        if offset is not None and not isinstance(offset, Offset):
            offset = Offset(offset)
        self._y = offset

    def _set_horizontal_inplace(self, offset: Union[Offset, Numeric, None]) -> None:
        if offset is not None and not isinstance(offset, Offset):
            offset = Offset(offset)
        self._x = offset

    vertical = property(get_vertical, _set_vertical_inplace)
    horizontal = property(get_horizontal, _set_horizontal_inplace)

    def get_x(self, default: Optional[float] = None) -> Optional[float]:
        if self.horizontal:
            return self.horizontal.x
        else:
            return default

    def get_y(self, default: Optional[float] = None) -> Optional[float]:
        if self.vertical:
            return self.vertical.x
        else:
            return default

    def _set_x_inplace(self, value: Numeric) -> None:
        if self.horizontal:
            self.horizontal.set_value(value, inplace=True)
        else:
            horizontal = Offset(value)
            self.set_horizontal(horizontal, inplace=True)

    def _set_y_inplace(self, value: Numeric) -> None:
        if self.vertical:
            self.vertical.set_value(value, inplace=True)
        else:
            vertical = Offset(value)
            self.set_vertical(vertical, inplace=True)

    x = property(get_x, _set_x_inplace)
    y = property(get_y, _set_y_inplace)

    def to_point(self) -> Point:
        unit_type = self.get_unit_type()
        screen_context = self.get_screen_context()
        assert self.vertical is not None and self.horizontal is not None
        y = self.vertical.get_units(unit_type, screen_context=screen_context)
        x = self.horizontal.get_units(unit_type, screen_context=screen_context)
        return Point(y, x, unit_type=unit_type, screen_context=screen_context)

    def get_unit_type(self, default: UnitType = UnitType.Auto) -> UnitType:
        if self.vertical:
            vertical_unit_type = self.vertical.unit_type
        else:
            vertical_unit_type = None
        if self.horizontal:
            horizontal_unit_type = self.horizontal.unit_type
        else:
            horizontal_unit_type = None
        if vertical_unit_type:
            return vertical_unit_type
        elif horizontal_unit_type:
            return horizontal_unit_type
        else:
            return default

    def get_screen_context(self, default: ScreenContext = ScreenContext.Auto):
        if self.vertical:
            vertical_screen_context = self.vertical.screen_context
        else:
            vertical_screen_context = None
        if self.horizontal:
            horizontal_screen_context = self.horizontal.screen_context
        else:
            horizontal_screen_context = None
        if vertical_screen_context:
            return vertical_screen_context
        elif horizontal_screen_context:
            return horizontal_screen_context
        else:
            return default

    unit_type = property(get_unit_type)
    screen_context = property(get_screen_context)

    def get_vertical_units(self, unit_type: UnitType = UnitType.Auto) -> Optional[float]:
        if unit_type is None or unit_type == UnitType.Auto:
            unit_type = self.get_unit_type()
        if self.vertical is not None:
            return self.vertical.get_units(unit_type)

    def get_horizontal_units(self, unit_type: UnitType = UnitType.Auto) -> Optional[float]:
        if unit_type is None or unit_type == UnitType.Auto:
            unit_type = self.get_unit_type()
        if self.horizontal is not None:
            return self.horizontal.get_units(unit_type)

    def get_lines_count(self, default: int = 1) -> int:
        if self.vertical is not None:
            return self.vertical.get_lines_count()
        else:
            return default

    def get_line_len(self, default: int = DEFAULT_LINE_LEN) -> int:
        if self.horizontal is not None:
            return self.horizontal.get_line_len()
        else:
            return default

    def get_value(self) -> Tuple[Offset, Offset]:
        return self.vertical, self.horizontal

    def __str__(self):
        return f'{self.vertical} x {self.horizontal}'
