from abc import ABC
from typing import Type, Optional, Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Class
    from base.functions.arguments import get_name
    from base.constants.chars import EMPTY, REPR_DELIMITER
    from base.classes.enum import DynamicEnum, ClassType
    from base.interfaces.data_interface import SimpleDataInterface
    from base.mixin.display_mixin import DisplayMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Class
    from ..functions.arguments import get_name
    from ..constants.chars import EMPTY, REPR_DELIMITER
    from ..classes.enum import DynamicEnum, ClassType
    from ..interfaces.data_interface import SimpleDataInterface
    from .display_mixin import DisplayMixin

Native = SimpleDataInterface
Key = Union[DynamicEnum, str]
Item = Any
ItemType = Union[Type, DynamicEnum, str]

DESCRIPTION_COL_NAMES = PREFIX, KEY, VALUE, CAPTION = 'prefix', 'key', 'value', 'caption'
DESCRIPTION_COL_LENS = 3, 10, 20, 85  # prefix, key, value, caption
DESCRIPTION_COLS = list(zip(DESCRIPTION_COL_NAMES, DESCRIPTION_COL_LENS))
UNK_COUNT_STUB = '<iter>'  # used in SimpleDataWrapper, IterDataMixin, ContextualData
DEFAULT_CHAPTER_TITLE_LEVEL = 3  # used in SimpleDataWrapper, ContextualData, FlatStruct


class DataMixin(DisplayMixin, ABC):
    def _get_data(self) -> Iterable:
        assert isinstance(self, SimpleDataInterface) or hasattr(self, 'get_data'), f'got {self}'
        return self.get_data()

    @staticmethod
    def _get_max_depth() -> int:
        return 0

    @staticmethod
    def _get_root_data_class() -> Class:
        return Any

    def _get_item_classes(self, level: int = -1) -> tuple:
        if level < 0:
            level = 1 + self._get_max_depth() - level
            return self._get_item_classes(level)
        if level == 0:
            return self._get_root_data_class(),
        else:
            template = 'Expected level from -{limit} to {limit}, got {value}'
            msg = template.format(limit=self._get_max_depth(), value=level)
            raise ValueError(self._get_call_prefix(self._get_item_classes, arg=level) + msg)

    def get_default_item_class(self, level: int = -1) -> Optional[Class]:
        item_classes = self._get_item_classes(level)
        if item_classes:
            return item_classes[0]

    def set_data(self, data: Iterable, inplace: bool, **kwargs) -> Native:
        data_root_class = self._get_root_data_class()
        if data_root_class:
            assert isinstance(data, data_root_class)
        parent = super()
        if isinstance(parent, SimpleDataInterface) or hasattr(parent, 'set_data'):
            kwargs['inplace'] = inplace
            return parent.set_data(data, **kwargs)
        else:
            raise AttributeError(self._get_call_prefix(self.set_data) + 'parent method not found')

    def has_data(self) -> bool:
        return bool(self._get_data())

    def is_defined(self) -> bool:
        return self.has_data()

    def is_empty(self) -> bool:
        return not self.has_data()

    def _get_call_prefix(
            self,
            method: Union[Callable, str, None] = None,
            arg: Any = EMPTY,
            delimiter: str = ': ',
    ) -> str:
        if method:
            template = '{cls}({name}).{method}({arg})'
            if hasattr(self, 'get_name'):
                name = repr(self.get_name())
            else:
                name = EMPTY
            method = get_name(method, or_callable=False)
            prefix = template.format(cls=self.__class__.__name__, name=name, method=method, arg=arg)
        else:
            prefix = repr(self)
        return prefix + delimiter
