from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream, ExtendedLoggerInterface,
        ContentFormatInterface, ContentType, ConnType, ItemType, StreamType, LoggingLevel,
        ARRAY_TYPES, AUTO, Auto, AutoBool, AutoName, AutoCount, Count, Name, OptionalFields,
    )
    from content.struct.flat_struct import FlatStruct
    from connectors.abstract.leaf_connector import LeafConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream, ExtendedLoggerInterface,
        ContentFormatInterface, ContentType, ConnType, ItemType, StreamType, LoggingLevel,
        ARRAY_TYPES, AUTO, Auto, AutoBool, AutoName, AutoCount, Count, Name, OptionalFields,
    )
    from ...content.struct.flat_struct import FlatStruct
    from ..abstract.leaf_connector import LeafConnector

Native = LeafConnector
Stream = Union[RegularStream, ColumnarInterface]
GeneralizedStruct = Union[StructInterface, list, tuple, Auto, None]


class Table(LeafConnector):
    def __init__(
            self,
            name: Name,
            struct: Union[StructInterface, Auto],
            database: ConnectorInterface,
            reconnect: bool = True,
            verbose: AutoBool = AUTO,
    ):
        super().__init__(
            name=name,
            struct=struct,
            parent=database,
            verbose=verbose,
        )
        if reconnect and hasattr(database, 'connect'):
            database.connect(reconnect=True)

    def get_content_type(self) -> ContentType:
        return ContentType.TsvFile

    @staticmethod
    def _get_detected_format_by_name(name: str) -> ContentFormatInterface:
        return ContentType.TsvFile

    def _get_detected_struct(self, set_struct: bool = False, verbose: AutoBool = AUTO) -> StructInterface:
        struct = self.get_struct_from_database(set_struct=set_struct)
        if not isinstance(struct, StructInterface) and arg.delayed_acquire(verbose, self.is_verbose):
            message = 'Struct as {} is deprecated. Use items.FlatStruct instead.'.format(type(struct))
            self.log(msg=message, level=LoggingLevel.Warning)
        return struct

    def get_database(self) -> ConnectorInterface:
        database = self.get_parent()
        return self._assume_connector(database)

    def get_count(self, verbose: AutoBool = AUTO) -> Count:
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        return database.select_count(self.get_name(), verbose=verbose)

    def get_columns(self) -> list:
        return self.get_struct().get_columns()

    def set_struct(self, struct: GeneralizedStruct, inplace: bool) -> Optional[Native]:
        if isinstance(struct, StructInterface) or struct is None:
            pass
        elif isinstance(struct, ARRAY_TYPES):
            if max([isinstance(f, ARRAY_TYPES) for f in struct]):
                struct = FlatStruct(struct)
            else:
                struct = FlatStruct.get_struct_detected_by_title_row(struct)
        elif struct == arg.AUTO:
            struct = self.get_struct_from_database()
        else:
            message = 'struct must be StructInterface or tuple with fields_description (got {})'.format(type(struct))
            raise TypeError(message)
        return super().set_struct(struct, inplace=inplace)

    def get_struct_from_database(self, set_struct: bool = False, skip_missing: bool = False) -> StructInterface:
        struct = FlatStruct(self.describe())
        if struct.is_empty() and not skip_missing:
            raise ValueError('Can not get struct for non-existing table {}'.format(self))
        if set_struct:
            self.set_struct(struct)
        return struct

    def get_first_line(self, close: bool = True, verbose: bool = True) -> Optional[str]:
        database = self.get_database()
        iter_lines = database.select(self.get_name(), '*', count=1, verbose=verbose)
        lines = list(iter_lines)
        if close:
            self.close()
        if lines:
            return lines[0]

    def get_rows(self, verbose: AutoBool = AUTO) -> Iterable:
        database = self.get_database()
        return database.select_all(self.get_name(), verbose=verbose)

    def get_data(self, verbose: AutoBool = AUTO) -> Iterable:
        return self.get_rows(verbose=verbose)

    def get_items(self, verbose: AutoBool = AUTO) -> Iterable:
        return self.get_rows(verbose=verbose)

    def get_items_of_type(
            self,
            item_type: Union[ItemType, Auto],
            verbose: AutoBool = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        item_type = arg.delayed_acquire(item_type, self.get_item_type)
        rows = self.get_rows(verbose=verbose)
        if item_type == ItemType.Row:
            items = rows
        else:
            if item_type == ItemType.StructRow:
                row_class = ItemType.StructRow.get_class()
                items = map(lambda i: row_class(i, self.get_struct()), rows)
            elif item_type == ItemType.Record:
                items = map(lambda r: {c: v for c, v in zip(r, self.get_columns())})
            elif item_type == ItemType.Line:
                items = map(lambda r: '\t'.join([str(v) for v in r]), rows)
            else:
                raise ValueError('Table.get_items_of_type(): cannot convert Rows to {}'.format(item_type))
        if step:
            logger = self.get_logger()
            if isinstance(logger, ExtendedLoggerInterface):
                count = self._get_fast_count()
                msg = 'Downloading {} lines from {}'.format(count, self.get_name())
                items = logger.progress(items, name=msg, count=count, step=step, context=self.get_context())
        return items

    def from_stream(self, stream, **kwargs):
        assert isinstance(stream, RegularStream)
        self.upload(data=stream, **kwargs)

    def is_existing(self) -> bool:
        database = self.get_database()
        return database.exists_table(self.get_path())

    def describe(self) -> Iterable:
        database = self.get_database()
        return database.describe_table(self.get_path())

    def create(self, drop_if_exists: bool, verbose: AutoBool = arg.AUTO):
        database = self.get_database()
        return database.create_table(
            self.get_name(),
            struct=self.get_struct(),
            drop_if_exists=drop_if_exists,
            verbose=verbose,
        )

    def upload(
            self, data: Union[Iterable, Stream],
            encoding: Optional[str] = None, skip_first_line: bool = False,
            skip_lines: int = 0, max_error_rate: float = 0.0,
            verbose: AutoBool = arg.AUTO,
    ):
        database = self.get_database()
        return database.safe_upload_table(
            self.get_name(),
            data=data,
            struct=self.get_struct(),
            skip_lines=skip_lines,
            skip_first_line=skip_first_line,
            encoding=encoding,
            max_error_rate=max_error_rate,
            verbose=verbose,
        )

    def is_empty(self) -> bool:
        count = self.get_count()
        return not count

    def select(self, *columns, **expressions) -> Stream:
        columns = arg.update(columns)
        if not expressions:
            is_simple_fields = min([isinstance(c, str) for c in columns])
            if is_simple_fields:
                return self.get_database().select(table=self, fields=columns)
        stream = self.to_struct_stream().select(*columns, **expressions)
        return self._assume_stream(stream)


ConnType.add_classes(Table)
