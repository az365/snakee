try:  # Assume we're a submodule in a package.
    from content.visuals.unit_type import UnitType
    from content.visuals.screen_context import ScreenContext
    from content.visuals.abstract_visual import AbstractVisual, Abstract2d
    from content.visuals.offset import Offset
    from content.visuals.point import Point
    from content.visuals.size import Size
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .unit_type import UnitType
    from .screen_context import ScreenContext
    from .abstract_visual import Numeric, AbstractVisual, Abstract2d
    from .offset import Offset
    from .point import Point
    from .size import Size
