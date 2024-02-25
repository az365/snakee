from typing import Optional, Iterable, Iterator, Tuple, Union

try:  # Assume we're a submodule in a package.
    from base.classes.enum import DynamicEnum
    from base.functions.arguments import get_name, get_value
    from base.functions.errors import get_type_err_msg
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.map_data_mixin import MapDataMixin
    from content.visuals.align import Align2d
    from content.visuals.size import Size
    from content.visuals.pair import VisualCell, PairSize, Size
    from content.visuals.align import Align2d, VerticalAlign, HorizontalAlign
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import DynamicEnum
    from ...base.functions.arguments import get_name, get_value
    from ...base.functions.errors import get_type_err_msg
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.mixin.map_data_mixin import MapDataMixin
    from ..visuals.align import Align2d
    from ..visuals.size import Size
    from ..visuals.pair import VisualCell, PairSize, Size
    from ..visuals.align import Align2d, VerticalAlign, HorizontalAlign

Native = Union[SimpleDataWrapper, MapDataMixin]
StyleData = dict
Key = str
Value = str
Item = Tuple[Key, Value]


class SimpleContentStyle(SimpleDataWrapper, MapDataMixin):
    def __init__(self, data: Optional[StyleData] = None, name: Optional[str] = None, **kwargs):
        super().__init__(data=StyleData(), name=name)
        self.add_data(data, inplace=True)
        self.add_data(kwargs, inplace=True)

    def get_data(self) -> StyleData:
        return super().get_data()

    def set_data(self, data: StyleData, inplace: bool, **kwargs) -> Native:
        assert isinstance(data, StyleData), TypeError(get_type_err_msg(data, expected=StyleData, arg='data'))
        result = super().set_data(data, inplace=inplace, **kwargs)
        return self._assume_native(result)

    def add_data(self, data: Union[dict, MapDataMixin, None], inplace: bool = True) -> Native:
        if data is None:
            data = dict()
        elif isinstance(data, MapDataMixin):
            data = data._get_data()  # dict
        assert isinstance(data, dict), get_type_err_msg(got=data, expected=dict, arg='data')
        if inplace:
            if data is not None:
                for k, v in data.items():
                    self.set_value(k, v)
            return self
        else:
            new_data = self.get_data().copy()
            if data is not None:
                new_data = new_data.update(data)
            return self.__class__(new_data)

    def get_items(self) -> Iterable[Item]:
        return self.get_data().items()

    def add_items(self, items: Iterable, before: bool = False, inplace: bool = False) -> Optional[Native]:
        data_dict = self.get_data()
        if not inplace:
            data_dict = data_dict.copy()
        for k, v in items:
            data_dict[k] = v
        if inplace:
            return self
        else:
            return self.__class__(data_dict, name=self.get_name())

    def get_value(self, field: Key, default: Optional[Value] = None) -> Value:
        data = self.get_data()
        return data.get(field, default)

    def set_value(self, key: Key, value: Value, inplace: bool = True) -> Native:
        data = self.get_data()
        if inplace:
            data[key] = value
            return self
        else:
            new_data = data.copy()
            new_data[key] = value
            return self.__class__(new_data)

    def get_css_items(self) -> Iterator[Item]:
        for k, v in super().get_items():
            yield str(k), str(v)

    def get_css_line(self) -> str:
        line = ''
        for k, v in self.get_css_items():
            line += f'{k}: {v}; '
        return line

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj


Native = SimpleContentStyle
MAIN_ATTRIBUTE_TYPES = Align2d, VisualCell


class AdvancedContentStyle(SimpleContentStyle):
    def __init__(
            self,
            data: Optional[dict] = None,
            align: Optional[Align2d] = None,
            cell: Optional[VisualCell] = None,
            **kwargs
    ):
        self.align = align
        self.cell = cell
        super().__init__(data, **kwargs)

    def get_vertical_align(self) -> Optional[VerticalAlign]:
        if self.align is None:
            return None
        elif isinstance(self.align, Align2d) or hasattr(self.align, 'get_vertical'):
            return self.align.get_vertical()
        else:
            msg = get_type_err_msg(got=self.align, expected=Align2d, arg='self.align')
            raise TypeError(msg)

    def get_horizontal_align(self) -> Optional[HorizontalAlign]:
        if self.align is None:
            return None
        elif isinstance(self.align, Align2d) or hasattr(self.align, 'get_horizontal'):
            return self.align.get_horizontal()
        else:
            msg = get_type_err_msg(got=self.align, expected=Align2d, arg='self.align')
            raise TypeError(msg)

    def get_main_attributes(self) -> Iterator:
        yield self.align
        yield self.cell

    def get_main_items(self) -> Iterator[Item]:
        for attribute in self.get_main_attributes():
            yield from attribute.get_attr_items()

    def get_simplified_main_items(self) -> Iterator[Item]:
        for i in self.get_main_attributes():
            if isinstance(i, MAIN_ATTRIBUTE_TYPES) or hasattr(i, 'get_simplified_items'):
                yield from i.get_simplified_items()
            elif i is not None:
                raise TypeError(get_type_err_msg(i, MAIN_ATTRIBUTE_TYPES, arg='i'))

    def get_additional_items(self) -> Iterator[Item]:
        return self.get_data().items()

    def get_items(self) -> Iterator[Item]:
        yield from self.get_main_items()
        yield from self.get_additional_items()

    def get_simple_content_style(self) -> SimpleContentStyle:
        content_style = SimpleContentStyle()
        content_style.add_items(self.get_simplified_main_items())
        content_style.add_items(self.get_additional_items())
        return content_style

    def get_css_items(self) -> Iterator[Item]:
        return self.get_simple_content_style().get_css_items()

    @staticmethod
    def _assume_native(obj) -> SimpleContentStyle:
        return obj
