try:  # Assume we're a submodule in a package.
    from content.visuals.visual_classes import UnitType, Offset, Point, Size
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .visual_classes import UnitType, Offset, Point, Size


def test_offset():
    line_len = 10
    font_size = 16
    font_proportion = 0.44
    px = int(line_len * font_size * font_proportion)  # 70
    em = round(line_len * font_proportion, 1)  # 4.4
    unit_type = UnitType.Symbol

    offset = Offset(line_len)
    assert offset.get_x() == line_len, f'{offset.get_x()} vs {line_len}'
    assert offset.get_unit_type() == unit_type, f'{offset.get_unit_type()} vs {unit_type}'
    assert offset.get_px() == px, f'{offset.get_px()} vs {px}'
    assert offset.get_em() == em, f'{offset.get_em()} vs {em}'

    for ut in (unit_type, UnitType.Pixel, UnitType.Width):
        offset.unit_type = ut
        prefix = f'unit_type={ut.value}:'
        assert offset.get_px() == px, f'{prefix} {offset.get_px()} vs {px}'
        assert offset.get_em() == em, f'{prefix} {offset.get_em()} vs {em}'

    em_offset = Offset('1em')
    px_offset = Offset('16px')
    assert em_offset == px_offset


def test_point():
    point0 = Point('1em', '16px')
    point1 = Point('16px', '1em')
    assert point0 == point1, f'{point0} vs {point1}'


def test_size():
    size = Size('1em', '16px')
    point = Point('16px', '1em')
    assert size == point, f'{size} vs {point}'


def main():
    test_offset()
    test_point()
    test_size()


if __name__ == '__main__':
    main()
