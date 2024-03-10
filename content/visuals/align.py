from typing import Optional, Iterator, Tuple, Union

try:  # Assume we're a submodule in a package.
    from base.classes.enum import DynamicEnum
    from base.functions.arguments import get_name, get_value
    from base.functions.errors import get_type_err_msg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import DynamicEnum
    from ...base.functions.arguments import get_name, get_value
    from ...base.functions.errors import get_type_err_msg

CssItem = Tuple[str, str]


class StyleValue(DynamicEnum):
    def get_css_value(self) -> str:
        value = self.get_value()
        return str(value)


class AbstractAlign(StyleValue):
    Begin = 'begin'
    End = 'end'
    Center = 'center'
    Justify = 'justify'
    Auto = None

    _dict_css = dict()

    def get_css_value(self) -> str:
        if self in self._dict_css:
            css_value = self._dict_css[self]
        else:
            cur_value = get_value(self)
            if cur_value in self._dict_css:
                css_value = self._dict_css[cur_value]
            else:
                abstract_align = AbstractAlign(cur_value)
                css_value = self._dict_css.get(abstract_align, cur_value)
        return css_value

    @staticmethod
    def get_css_name():
        msg = get_type_err_msg(expected=(VerticalAlign, HorizontalAlign), got=AbstractAlign, arg='self')
        raise TypeError(msg)

    def get_css_items(self) -> Iterator[CssItem]:
        yield self.get_css_name(), self.get_css_value()

    def get_items(self) -> Iterator[CssItem]:
        yield from self.get_css_items()

    @classmethod
    def convert(
            cls,
            obj: Union[DynamicEnum, str],
            default: Optional[DynamicEnum] = None,
            skip_missing: bool = False,
    ) -> StyleValue:
        if obj is None:
            return cls.Auto
        else:
            return super().convert(obj, default=default, skip_missing=skip_missing)

    @classmethod
    def from_str(cls, obj: str):
        if obj in cls._dict_css.values():
            for k, v in cls._dict_css.items():
                if obj == v:
                    canonic_value = get_value(k)
                    return cls(canonic_value)
        else:
            for i in cls.get_enum_items():
                name = get_name(i)
                value = get_value(i)
                if obj == name or obj == value:
                    return i

    @classmethod
    def from_any(cls, obj):
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, AbstractAlign):
            return cls(get_value(obj))
        elif obj is None:
            return cls.get_default()
        elif isinstance(obj, str):
            return cls.from_str(obj)


class VerticalAlign(AbstractAlign):
    Top = AbstractAlign.Begin
    Center = AbstractAlign.Center
    Bottom = AbstractAlign.End
    Auto = None

    _dict_css = {
        AbstractAlign.Begin: 'top',
        AbstractAlign.End: 'bottom',
    }

    @staticmethod
    def get_css_name():
        return 'vertical-align'


class HorizontalAlign(AbstractAlign):
    Left = AbstractAlign.Begin
    Center = AbstractAlign.Center
    Right = AbstractAlign.End
    Auto = None

    _dict_css = {
        AbstractAlign.Begin: 'left',
        AbstractAlign.End: 'right',
    }

    @staticmethod
    def get_css_name():
        return 'text-align'


VerticalAlign.prepare()
HorizontalAlign.prepare()


class Align2d:
    def __init__(self, vertical: Optional[AbstractAlign] = None, horizontal: Optional[AbstractAlign] = None):
        assert not isinstance(vertical, HorizontalAlign)
        assert not isinstance(horizontal, VerticalAlign)
        self.vertical = VerticalAlign.from_any(vertical)
        self.horizontal = HorizontalAlign.from_any(horizontal)

    def get_vertical(self) -> VerticalAlign:
        value = get_value(self.vertical)
        return VerticalAlign.from_str(value)

    def get_horizontal(self) -> HorizontalAlign:
        value = get_value(self.horizontal)
        return HorizontalAlign.from_str(value)

    def get_attributes(self) -> Iterator[AbstractAlign]:
        yield self.get_vertical()
        yield self.get_horizontal()

    def get_attr_items(self) -> Iterator[tuple]:
        for i in self.get_attributes():
            if i is not None:
                yield from i.get_items()

    def get_simplified_items(self) -> Iterator[tuple]:
        for k, v in self.get_attr_items():
            if v is not None:
                yield k, v

    def get_css_items(self) -> Iterator[CssItem]:
        for i in self.get_vertical(), self.get_horizontal():
            yield from i.get_css_items()

    def get_css_line(self) -> str:
        line = ''
        for k, v in self.get_css_items():
            line += f'{k}: {v}; '
        return line

    def get_names(self) -> Iterator[str]:
        for i in self.get_attributes():
            if i is None:
                value = AbstractAlign.Auto
            else:
                value = i
            yield get_name(value)

    def get_values(self) -> Iterator[str]:
        for i in self.get_attributes():
            if i is None:
                yield i
            else:
                yield i.get_css_value()

    def __str__(self):
        ver, hor = self.get_values()
        return f'{ver} {hor}'

    def __repr__(self):
        ver, hor = self.get_names()
        cls_name = self.__class__.__name__
        return f'{cls_name}({ver}, {hor})'
