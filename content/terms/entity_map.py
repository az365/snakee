from typing import Optional, Generator

try:  # Assume we're a submodule in a package.
    from interfaces import AUTO, Auto, AutoCount
    from base.constants.chars import EMPTY, SMALL_INDENT, TAB_INDENT, REPR_DELIMITER, DEFAULT_LINE_LEN
    from base.abstract.simple_data import SimpleDataWrapper, AutoDisplay
    from utils.external import DataFrame
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import AUTO, Auto, AutoCount
    from ...base.constants.chars import EMPTY, SMALL_INDENT, TAB_INDENT, REPR_DELIMITER, DEFAULT_LINE_LEN
    from ...base.abstract.simple_data import SimpleDataWrapper, AutoDisplay
    from ...utils.external import DataFrame

Native = SimpleDataWrapper

COLS_FOR_ENTITY = ('defined', 3), ('key', 20), ('value', 30), ('actual_type', 20), ('caption', 50)
NOT_ENTITY_FIELDS = 'name', 'caption'


class EntityMap(SimpleDataWrapper):
    def has_data(self) -> bool:
        return bool(self.get_count())

    def get_entities(self):
        return self.get_meta(ex=NOT_ENTITY_FIELDS)

    def get_entity_records(self):
        return self.get_meta_records(ex=NOT_ENTITY_FIELDS)

    def get_count(self) -> int:
        return len(self.get_entities())

    def get_count_repr(self, default: str = 'N/A') -> str:
        count = self.get_count() or default
        return '{count} entities'.format(count=count)

    def display_meta(
            self,
            with_title: bool = True,
            with_summary: bool = True,
            prefix: str = SMALL_INDENT,
            delimiter: str = REPR_DELIMITER,
            display: AutoDisplay = AUTO,
    ) -> Native:
        display = self.get_display(display)
        display.display_paragraph('{prefix}{count}:'.format(prefix=prefix, count=self.get_count_repr()))
        return self

    def display_data_sheet(
            self,
            count: AutoCount = AUTO,
            title: Optional[str] = 'Data:',
            comment: Optional[str] = None,
            max_len: AutoCount = AUTO,
            display: AutoDisplay = AUTO,
    ):
        display = self.get_display(display)
        if comment:
            display.display_paragraph(comment)
        return display.display_sheet(
            records=self.get_entity_records(),
            columns=COLS_FOR_ENTITY,
            with_title=True,
        )

    def get_str_headers(self) -> Generator:
        yield from self.get_brief_meta_description()
        yield EMPTY

    def show(self, **kwargs):
        records = self.get_entity_records()
        columns = [k for k, v in COLS_FOR_ENTITY]
        return DataFrame(records, columns=columns)
