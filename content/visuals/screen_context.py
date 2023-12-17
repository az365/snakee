from typing import Union

Numeric = Union[int, float]
NUMERIC_TYPES = int, float

DEFAULT_FONT_SIZE = 16
DEFAULT_FRONT_PROPORTION = 0.44
DEFAULT_SYMBOL_WIDTH = 7
DEFAULT_CONTAINER_WIDTH, DEFAULT_CONTAINER_HEIGHT = 640, 480
DEFAULT_LINE_LEN = DEFAULT_CONTAINER_WIDTH / DEFAULT_SYMBOL_WIDTH


class ScreenContext:
    Auto = None

    def __init__(
            self,
            height: Numeric = DEFAULT_CONTAINER_HEIGHT,
            width: Numeric = DEFAULT_CONTAINER_WIDTH,
            font_size: Numeric = DEFAULT_FONT_SIZE,
            font_proportion: Numeric = DEFAULT_FRONT_PROPORTION,
    ):
        self.height = height
        self.width = width
        self.font_size = font_size
        self.font_proportion = font_proportion

    def get_mean_symbol_width(self) -> float:
        return self.font_size * self.font_proportion

    mean_symbol_width = property(get_mean_symbol_width)
