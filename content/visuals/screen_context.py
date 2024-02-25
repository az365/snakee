try:  # Assume we're a submodule in a package.
    from base.classes.typing import NUMERIC_TYPES, Numeric
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import NUMERIC_TYPES, Numeric

DEFAULT_FONT_SIZE = 16
DEFAULT_FRONT_PROPORTION = 0.44
DEFAULT_SYMBOL_WIDTH = round(DEFAULT_FONT_SIZE * DEFAULT_FRONT_PROPORTION, 1)  # 7.0
DEFAULT_CONTAINER_WIDTH, DEFAULT_CONTAINER_HEIGHT = 640, 480
DEFAULT_LINE_LEN = int(DEFAULT_CONTAINER_WIDTH / DEFAULT_SYMBOL_WIDTH)  # 91


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
