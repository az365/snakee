from typing import Iterable

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Class
    from base.constants.chars import EMPTY
    from base.abstract.simple_data import SimpleDataWrapper
    from base.interfaces.sheet_interface import SheetInterface, Record, Row, Columns
    from base.mixin.iter_data_mixin import IterDataMixin
    from base.mixin.sheet_mixin import SheetMixin, SheetItems, Native
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Class
    from ..constants.chars import EMPTY
    from ..abstract.simple_data import SimpleDataWrapper
    from ..interfaces.sheet_interface import SheetInterface, Record, Row, Columns
    from ..mixin.iter_data_mixin import IterDataMixin
    from ..mixin.sheet_mixin import SheetMixin, SheetItems, Native


class SimpleSheet(SimpleDataWrapper, IterDataMixin, SheetMixin, SheetInterface):
    _sheet_class: Class = None

    def __init__(self, data: Iterable, columns: Columns, name: str = EMPTY, caption: str = EMPTY):
        self._column_names = list()
        self._column_lens = list()
        super().__init__(list(), name=name, caption=caption)
        self._set_columns_inplace(columns)
        self._set_items_inplace(data)

    @classmethod
    def get_class(cls) -> Class:
        if not cls._sheet_class:
            cls._sheet_class = cls
        return cls._sheet_class

    @classmethod
    def set_class(cls, sheet_class: Class) -> Class:
        cls._sheet_class = sheet_class
        return sheet_class
