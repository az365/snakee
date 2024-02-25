from typing import Union

try:  # Assume we're a submodule in a package.
    from content.visuals.unit_type import UnitType
    from content.visuals.screen_context import ScreenContext, Numeric, NUMERIC_TYPES, DEFAULT_LINE_LEN
    from content.visuals.abstract_visual import Abstract2d
    from content.visuals.offset import Offset
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .unit_type import UnitType
    from .screen_context import ScreenContext, Numeric, NUMERIC_TYPES, DEFAULT_LINE_LEN
    from .abstract_visual import Abstract2d
    from .offset import Offset

Native = Abstract2d
OffsetValue = Union[float, str]
Value = Union[tuple, str]
DEFAULT_VALUE = 0
COORD_DELIMITER = ' x '


class Point(Offset, Abstract2d):
    def __init__(
            self,
            y: OffsetValue,  # float, str
            x: OffsetValue,  # float, str
            unit_type: UnitType = UnitType.Auto,
            screen_context: ScreenContext = ScreenContext.Auto,
    ):
        super().__init__(DEFAULT_VALUE, unit_type=unit_type, screen_context=screen_context)
        self._y = DEFAULT_VALUE
        self.set_y(y, inplace=True)
        self.set_x(x, inplace=True)
        self.set_screen_context(screen_context, inplace=True)
        self.set_unit_type(unit_type, inplace=True, skip_empty=True)

    @classmethod
    def from_size(cls, size: Abstract2d) -> Native:
        unit_type = size.get_unit_type()
        screen_context = size.get_screen_context()
        y = size.get_vertical_units(unit_type)
        x = size.get_horizontal_units(unit_type)
        return Point(y, x, unit_type=unit_type, screen_context=screen_context)

    @classmethod
    def from_tuple(cls, value: Union[list, tuple]) -> Native:
        count = len(value)
        if count == 1:
            value = value, 0
            count = 2
        if 2 <= count <= 4:
            y, x = value[:2]
            point = Point(y, x)
            if count >= 4:
                screen_context = value[3]
                point.set_screen_context(screen_context, inplace=True)
            if count >= 3:
                unit_type = value[2]
                point.set_unit_type(unit_type, inplace=True)
            return point
        else:
            raise ValueError(value)

    @classmethod
    def from_str(
            cls,
            value: str,
            delimiter: str = COORD_DELIMITER,
            default_x: Numeric = 0,
            expected_unit_type: UnitType = UnitType.Auto,
            screen_context: ScreenContext = ScreenContext.Auto,
    ):
        if delimiter in value:
            y_str, x_str = value.split(delimiter)
            point = Point.from_tuple([y_str, x_str])
        else:
            y, unit_type = UnitType.parse(value, expected_unit_type=expected_unit_type)
            point = Point(y, default_x, unit_type=unit_type, screen_context=screen_context)
        return point

    def get_y(self) -> Numeric:
        return self._y

    def set_y(self, value: OffsetValue, inplace: bool):
        if inplace:
            self._set_y_inplace(value)
            return self
        else:
            return self.__class__(y=value, x=self.x, unit_type=self._unit_type, screen_context=self._screen_context)

    def _set_y_inplace(self, value: OffsetValue) -> None:
        if isinstance(value, NUMERIC_TYPES):
            self._y = value
        elif isinstance(value, str):
            y, unit_type = UnitType.parse(value)
            self.set_unit_type(unit_type, inplace=True, skip_empty=True)
            self._y = y
        else:
            raise TypeError(value)

    def _set_x_inplace(self, value: Numeric) -> None:
        if isinstance(value, NUMERIC_TYPES):
            self._x = value
        elif isinstance(value, str):
            x, unit_type = UnitType.parse(value)
            self.set_unit_type(unit_type, inplace=True, skip_empty=True)
            self._x = x
        else:
            raise TypeError(value)

    y = property(get_y, _set_y_inplace)

    def get_unit_type(self) -> UnitType:
        return super().get_unit_type()

    def _set_unit_type_inplace(self, unit_type: UnitType) -> None:
        if self._unit_type is None or self._unit_type == UnitType.Auto:
            self._unit_type = unit_type
        elif unit_type is None or unit_type == UnitType.Auto:
            raise ValueError(unit_type)
        else:
            self._x = UnitType.convert(self._x, src=self._unit_type, dst=unit_type, screen_context=self.screen_context)
            self._y = UnitType.convert(self._y, src=self._unit_type, dst=unit_type, screen_context=self.screen_context)
            self._unit_type = unit_type

    unit_type = property(get_unit_type, _set_unit_type_inplace)

    def get_vertical(self) -> Offset:
        return Offset(self.x, unit_type=self.unit_type, screen_context=self.screen_context)

    def get_horizontal(self) -> Offset:
        return Offset(self.x, unit_type=self.unit_type, screen_context=self.screen_context)

    vertical = property(get_vertical)
    horizontal = property(get_horizontal)

    def get_value(self) -> Value:
        if self.unit_type is None or self.unit_type == UnitType.Auto:
            return self.x, self.y
        else:
            return str(self)

    def set_value(self, value: Value, inplace: bool) -> Abstract2d:
        if inplace:
            self._set_value_inplace(value)
            return self
        else:
            return self.from_any(value, expected_unit_type=self.unit_type, screen_context=self.screen_context)

    def _set_value_inplace(self, value: Value) -> None:
        if isinstance(value, NUMERIC_TYPES):
            self._y = value
            self._x = None
        elif isinstance(value, (list, tuple)):
            self._set_tuple_inplace(value)
        elif isinstance(value, str):
            self._set_str_inplace(value)

    def set_tuple(self, value: Union[list, tuple], inplace: bool) -> Abstract2d:
        if inplace:
            self._set_tuple_inplace(value)
        else:
            return self.from_tuple(value)

    def _set_tuple_inplace(self, value: Union[list, tuple]) -> None:
        count = len(value)
        if count == 1:
            self._set_value_inplace(value[0])
        elif 2 <= count <= 4:
            self.set_y(value[0], inplace=True)
            self.set_x(value[1], inplace=True)
            if count >= 3:
                self.set_unit_type(value[2], inplace=True)
            if count >= 4:
                self.set_screen_context(value[3], inplace=True)
        else:
            raise ValueError(f'expected 2..4 items, got {count} ({value})')

    def set_str(self, value: str, inplace: bool, delimiter: str = COORD_DELIMITER) -> Abstract2d:
        if inplace:
            self._set_str_inplace(value)
            return self
        else:
            return self.from_str(
                value, delimiter=delimiter,
                expected_unit_type=self.unit_type, screen_context=self.screen_context,
            )

    def _set_str_inplace(self, value: str, delimiter: str = COORD_DELIMITER):
        assert isinstance(value, str), value
        if delimiter in value:
            y_str, x_str = value.split(delimiter)
            y, y_unit_type = UnitType.parse(y_str)
            x, x_unit_type = UnitType.parse(x_str)
            if y_unit_type:
                self.set_unit_type(y_unit_type, inplace=True)
            self.set_y(y, inplace=True)
            if x_unit_type:
                self.set_unit_type(x_unit_type, inplace=True)
            self.set_x(x, inplace=True)
        else:
            x, unit_type = UnitType.parse(value)
            self.set_x(x, inplace=True)
            self.set_unit_type(unit_type, inplace=True)

    def get_lines_count(self) -> int:
        return self.vertical.get_lines_count()

    def get_line_len(self) -> int:
        return self.horizontal.get_line_len()

    def __str__(self):
        if self.unit_type:
            return f'{self.y}{COORD_DELIMITER}{self.x} {self.unit_type.value}'
        else:
            return f'{self.y}{COORD_DELIMITER}{self.x}'
