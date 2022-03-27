from typing import Optional, Generator

try:  # Assume we're a submodule in a package.
    from interfaces import AUTO, Auto, AutoCount
    from base.constants.chars import EMPTY, SMALL_INDENT, PY_INDENT, REPR_DELIMITER, DEFAULT_LINE_LEN
    from base.abstract.simple_data import SimpleDataWrapper
    from utils.external import DataFrame
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import AUTO, Auto, AutoCount
    from ...base.constants.chars import EMPTY, SMALL_INDENT, PY_INDENT, REPR_DELIMITER, DEFAULT_LINE_LEN
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...utils.external import DataFrame

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

    def get_meta_description(
            self,
            with_title: bool = True,
            with_summary: bool = True,
            prefix: str = EMPTY,
            delimiter: str = REPR_DELIMITER,
    ) -> Generator:
        yield '{prefix}{count}:'.format(prefix=prefix, count=self.get_count_repr())
        yield EMPTY

    def get_data_description(
            self,
            count: AutoCount = AUTO,
            title: Optional[str] = 'Data:',
            max_len: AutoCount = AUTO,
    ) -> Generator:
        yield from self._get_columnar_lines(
            records=self.get_entity_records(),
            columns=COLS_FOR_ENTITY,
            with_title=True,
            prefix=PY_INDENT,
            delimiter=REPR_DELIMITER,
            max_len=max_len,
        )

    def get_str_headers(self) -> Generator:
        yield self.get_brief_repr()
        yield from self.get_brief_meta_description()
        yield EMPTY

    def show(self, **kwargs):
        records = self.get_entity_records()
        columns = [k for k, v in COLS_FOR_ENTITY]
        return DataFrame(records, columns=columns)
