from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_name
    from interfaces import StreamInterface, ColumnarInterface, Field, Columns
    from utils.external import pd, DataFrame
    from streams.abstract.wrapper_stream import WrapperStream
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.mixin.convert_mixin import ConvertMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.functions.arguments import get_name
    from ...interfaces import StreamInterface, ColumnarInterface, Field, Columns
    from ...utils.external import pd, DataFrame
    from ..abstract.wrapper_stream import WrapperStream
    from ..mixin.columnar_mixin import ColumnarMixin
    from ..mixin.convert_mixin import ConvertMixin

Native = Union[StreamInterface, ColumnarInterface]


class PandasStream(WrapperStream, ColumnarMixin, ConvertMixin):
    def __init__(
            self,
            data: Union[DataFrame, StreamInterface, Iterable],
            name: Optional[str] = None,
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

    def get_dataframe(self, columns: Columns = None) -> DataFrame:
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

    def _select_columns(self, columns: Columns) -> Native:
        if columns:
            return self.select(*columns)
        else:
            return self

    def get_records(self, columns: Columns = None) -> Iterable:
        stream = self._select_columns(columns)
        assert isinstance(stream, PandasStream)
        return stream._get_mapped_items(lambda i: i[1].to_dict())

    def get_rows(self, columns: Columns = None):
        stream = self._select_columns(columns)
        assert isinstance(stream, PandasStream)
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
        column_name = get_name(column)
        return self.get_dataframe()[column_name]

    def add_dataframe(self, dataframe: DataFrame, before: bool = False, inplace: bool = False) -> Native:
        if before:
            frames = [dataframe, self.get_dataframe()]
        else:
            frames = [self.get_dataframe(), dataframe]
        concatenated = pd.concat(frames)
        if inplace:
            self.set_data(concatenated)
            return self
        else:
            return self.stream(concatenated)

    def add_items(self, items: Iterable, before: bool = False, inplace: bool = False) -> Native:
        dataframe = DataFrame(items)
        return self.add_dataframe(dataframe, before=before, inplace=inplace)

    def add_stream(self, stream: StreamInterface, before: bool = False, inplace: bool = False) -> Native:
        if isinstance(stream, PandasStream):
            return self.add_dataframe(stream.get_data(), before=before, inplace=inplace)
        else:
            return self.add_items(stream.get_items(), before=before, inplace=inplace)

    def add(self, data: Union[DataFrame, StreamInterface, Iterable], before: bool = False, **kwargs) -> Native:
        assert not kwargs, f'PandasStream.add(): kwargs for PandasStream.add() not supported, got {kwargs}'
        if isinstance(data, DataFrame):
            return self.add_dataframe(data, before)
        elif isinstance(data, StreamInterface) or hasattr(data, 'get_items'):
            return self.add_stream(data, before)
        elif isinstance(data, Iterable):
            return self.add_items(data)
        else:
            raise TypeError(f'data must be DataFrame, Stream or Iterable, got {data}')

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
