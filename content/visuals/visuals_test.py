try:  # Assume we're a submodule in a package.
    from content.visuals.visual_classes import UnitType, Offset, Point, Size, Align2d
    from content.documents.content_style import AdvancedContentStyle
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .visual_classes import UnitType, Offset, Point, Size, Align2d
    from ..documents.content_style import AdvancedContentStyle


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


def test_align():
    align = Align2d('top', 'left')
    received = align.get_css_line()
    expected = 'vertical-align: top; text-align: left; '
    assert received == expected, f'{received} vs {expected}'
    style = AdvancedContentStyle({'font-size': '16px'}, align=align, color='green')
    received_style = style.get_css_line()
    expected_style = 'vertical-align: top; text-align: left; font-size: 16px; color: green; '
    assert received_style == expected_style, f'{received_style} vs {expected_style}'


def main():
    test_offset()
    test_point()
    test_size()
    test_align()


if __name__ == '__main__':
    main()
