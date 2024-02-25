from typing import Iterator

try:  # Assume we're a submodule in a package.
    from content.visuals.size import Size
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .size import Size


class PairSize:
    def __init__(self, first: Size, second: Size):
        self.first = first
        self.second = second

    def get_first(self) -> Size:
        return self.first

    def get_second(self) -> Size:
        return self.second

    def get_sum(self) -> Size:
        vertical = self.get_first().get_vertical() + self.get_second().get_vertical()
        horizontal = self.get_first().get_horizontal() + self.get_second().get_horizontal()
        return Size(vertical, horizontal)

    def get_attr_items(self) -> Iterator:
        yield 'first', self.first
        yield 'second', self.second

    def get_simplified_items(self) -> Iterator[tuple]:
        yield 'top', self.first.get_vertical()
        yield 'bottom', self.second.get_vertical()
        yield 'left', self.first.get_horizontal()
        yield 'right', self.second.get_horizontal()


class VisualCell:
    def __init__(self, margin: PairSize, border: PairSize, padding: PairSize):
        self.margin = margin
        self.border = border
        self.padding = padding

    def get_attr_items(self) -> Iterator[tuple]:
        yield 'margin', self.margin
        yield 'border', self.border
        yield 'padding', self.padding

    def get_simplified_items(self) -> Iterator[tuple]:
        """
        Extracting item-paris for attributes: margin-top, margin-bottom, margin-left, margin-right, ... padding-right.
        Can be used for CSS/HTML-attributes.
        :return: Iterator of tuples (attribute_name, attribute_value)
        """
        for space_name, space_value in self.get_attr_items():
            assert isinstance(space_value, PairSize) or hasattr(space_value, 'get_simplified_items'), space_value
            for direction_name, direction_value in space_value.get_simplified_items():
                if direction_value is not None:
                    key = f'{space_name}-{direction_name}'
                    yield key, direction_value

    def get_css_items(self) -> Iterator[tuple]:
        yield from self.get_simplified_items()
