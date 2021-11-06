from typing import Optional, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import pd, DataFrame
    from interfaces import StreamInterface, ColumnarInterface, Field
    from streams import stream_classes as sm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import pd, DataFrame
    from ...interfaces import StreamInterface, ColumnarInterface, Field
    from .. import stream_classes as sm

Native = Union[StreamInterface, ColumnarInterface]


class PandasStream(sm.WrapperStream, sm.ColumnarMixin, sm.ConvertMixin):
    def __init__(
            self,
            data,
            name=arg.AUTO,
            source=None,
            context=None,
    ):
        assert pd, 'Pandas must be installed and imported for instantiate PandasStream (got fallback {})'.format(pd)
        if isinstance(data, DataFrame) or data.__class__.__name__ == 'DataFrame':
            dataframe = data
        elif hasattr(data, 'get_dataframe'):  # isinstance(data, RecordStream):
            dataframe = data.get_dataframe()
        else:  # isinstance(data, (list, tuple)):
            dataframe = DataFrame(data=data)
        super().__init__(
            dataframe,
            name=name,
            source=source,
            context=context,
        )

    def get_data(self) -> DataFrame:
        return super().get_data()

    @staticmethod
    def get_item_type():
        return pd.Series

    @classmethod
    def _is_valid_item(cls, item) -> bool:
        return isinstance(item, pd.Series)

    def is_in_memory(self) -> bool:
        return True

    def get_dataframe(self, columns=None) -> DataFrame:
        data = self.get_data()
        assert isinstance(data, DataFrame)
        if columns:
            data = data[columns]
        return data

    def get_count(self, final: bool = False) -> Optional[int]:
        data = self.get_data()
        assert isinstance(data, DataFrame)
        return data.shape[0]

    def get_items(self) -> Iterable:
        yield from self.get_dataframe().iterrows()

    def get_records(self, columns=arg.AUTO) -> Iterable:
        stream = self.select(*columns) if arg.is_defined(columns) else self
        return stream._get_mapped_items(lambda i: i[1].to_dict())

    def get_rows(self, columns=arg.AUTO):
        stream = self.select(*columns) if arg.is_defined(columns) else self
        return stream._get_mapped_items(lambda i: i[1].to_list())

    def get_columns(self) -> Iterable:
        return self.get_dataframe().columns

    def get_expected_count(self) -> int:
        return self.get_dataframe().shape[0]

    def take(self, count: Union[int, bool] = 1) -> Native:
        if isinstance(count, bool):
            if count:
                return self
            else:
                return self.stream(DataFrame())
        elif isinstance(count, int):
            return self.stream(
                self.get_dataframe().head(count),
            )
        else:
            raise TypeError('Expected count as int or bool, got {}'.format(count))

    def get_one_column_values(self, column: Field) -> Iterable:
        column_name = arg.get_name(column)
        return self.get_dataframe()[column_name]

    def add_dataframe(self, dataframe: DataFrame, before=False) -> Native:
        if before:
            frames = [dataframe, self.get_dataframe()]
        else:
            frames = [self.get_dataframe(), dataframe]
        concatenated = pd.concat(frames)
        return self.stream(concatenated)

    def add_items(self, items: Iterable, before: bool = False) -> Native:
        dataframe = DataFrame(items)
        return self.add_dataframe(dataframe, before)

    def add_stream(self, stream: StreamInterface, before: bool = False) -> Native:
        if isinstance(stream, PandasStream):
            return self.add_dataframe(stream.get_data(), before=before)
        else:
            return self.add_items(stream.get_items(), before=before)

    def add(self, dataframe_or_stream_or_items, before: bool = False, **kwargs) -> Native:
        assert not kwargs, 'kwargs for PandasStream.add() not supported'
        if isinstance(dataframe_or_stream_or_items, DataFrame):
            return self.add_dataframe(dataframe_or_stream_or_items, before)
        elif isinstance(dataframe_or_stream_or_items, StreamInterface) or sm.is_stream(dataframe_or_stream_or_items):
            return self.add_stream(dataframe_or_stream_or_items, before)
        elif isinstance(dataframe_or_stream_or_items, Iterable):
            return self.add_items(dataframe_or_stream_or_items)
        else:
            msg = 'dataframe_or_stream_or_items must be DataFrame, Stream or Iterable, got {}'
            raise TypeError(msg.format(dataframe_or_stream_or_items))

    def select(self, *fields, **expressions) -> Native:
        assert not expressions, 'custom expressions are not implemented yet'
        dataframe = self.get_dataframe(columns=fields)
        return self.stream(dataframe)

    def filter(self, *filters, **expressions) -> Native:
        assert not filters, 'custom filters are not implemented yet'
        pandas_filter = None
        for k, v in expressions.items():
            one_filter = self.get_one_column_values(k) == v
            if pandas_filter:
                pandas_filter = pandas_filter & one_filter
            else:
                pandas_filter = one_filter
        if pandas_filter:
            data = self.get_data()[pandas_filter]
            return self.stream(data)
        else:
            return self

    def sort(self, *keys, reverse: bool = False) -> Native:
        dataframe = self.get_dataframe().sort_values(
            by=keys,
            ascending=not reverse,
        )
        return self.stream(dataframe)

    def group_by(self, *keys, as_pairs: bool = False) -> Native:
        grouped = self.get_dataframe().groupby(
            by=keys,
            as_index=as_pairs,
        )
        return self.stream(grouped)

    def is_empty(self) -> bool:
        return self.get_count() == 0

    def collect(self) -> Native:
        return self
