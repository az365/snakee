from enum import Enum
from typing import Optional, Tuple, Any

try:  # Assume we're a submodule in a package.
    from content.visuals.screen_context import ScreenContext, Numeric, NUMERIC_TYPES
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .screen_context import ScreenContext, Numeric, NUMERIC_TYPES


class UnitType(Enum):
    Pixel = 'px'
    Font = 'em'
    Percent = '%'
    Width = 'width'
    Symbol = 'symbol'
    Auto = None

    @classmethod
    def get_default(cls):
        return cls.Symbol

    def supports_html(self) -> bool:
        if self.value in ('px', 'em', '%'):
            return True
        else:
            return False

    @classmethod
    def convert(cls, value: Numeric, src, dst, screen_context: ScreenContext) -> Numeric:
        assert isinstance(value, NUMERIC_TYPES), value
        assert isinstance(src, UnitType), src
        assert isinstance(dst, UnitType), dst
        if src == dst:
            result = value
        elif src == UnitType.Percent:
            result = cls.convert(value, src=UnitType.Width, dst=dst, screen_context=screen_context) / 100
        elif dst == UnitType.Percent:
            result = cls.convert(value, src=src, dst=UnitType.Width, screen_context=screen_context) * 100
        else:
            assert src is not None and src != UnitType.Auto, src
            assert dst is not None and dst != UnitType.Auto, dst
            assert screen_context is not None and screen_context != ScreenContext.Auto, screen_context
            result = None

        if src == UnitType.Pixel:
            if dst == UnitType.Font:
                result = value / screen_context.font_size
            elif dst == UnitType.Symbol:
                result = value / screen_context.mean_symbol_width
            elif dst == UnitType.Width:
                result = value / screen_context.width
        elif src == UnitType.Font:
            if dst == UnitType.Pixel:
                result = value * screen_context.font_size
            elif dst == UnitType.Symbol:
                result = value / screen_context.font_proportion
            elif dst == UnitType.Width:
                result = value * screen_context.font_size / screen_context.width
        elif src == UnitType.Symbol:
            if dst == UnitType.Pixel:
                result = value * screen_context.font_size * screen_context.font_proportion
            elif dst == UnitType.Font:
                result = value * screen_context.font_proportion
            elif dst == UnitType.Width:
                result = value * screen_context.font_size * screen_context.font_proportion / screen_context.width
        elif src == UnitType.Width:
            if dst == UnitType.Pixel:
                result = value * screen_context.width
            elif dst == UnitType.Font:
                result = value * screen_context.width / screen_context.font_size
            elif dst == UnitType.Symbol:
                result = value * screen_context.width / screen_context.mean_symbol_width

        if result is None:
            raise ValueError(f'can not convert from {src} to {dst} (value={value})')
        else:
            return result

    @classmethod
    def parse(cls, line: str, default_number=0, expected_unit_type=None) -> Tuple[float, Any]:
        number, unit_type_str_from_y = cls.split_to_number_and_suffix(line, default_number=default_number)
        if unit_type_str_from_y:
            unit_type_from_y = UnitType(unit_type_str_from_y)
            if expected_unit_type and expected_unit_type != UnitType.Auto:
                assert unit_type_from_y == expected_unit_type, f'{unit_type_from_y} vs {expected_unit_type}'
            unit_type = unit_type_from_y
        else:
            unit_type = cls.get_default()
        return number, unit_type

    @staticmethod
    def split_to_number_and_suffix(line: str, default_number=None) -> Tuple[Optional[float], str]:
        for n in range(len(line), 0, -1):
            numeric_candidate = line[0: n]
            try:
                number = float(numeric_candidate)
                suffix = line[n:]
                return number, suffix
            except ValueError:
                pass
        return default_number, line
