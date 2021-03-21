from typing import Optional
import pandas as pd

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import arguments as arg


class PandasStream(sm.WrapperStream, sm.ColumnarMixin, sm.ConvertMixin):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            source=None,
            context=None,
    ):
        if isinstance(data, pd.DataFrame):
            dataframe = data
        elif isinstance(data, sm.RecordStream):
            dataframe = data.get_dataframe()
        else:  # isinstance(data, (list, tuple)):
            dataframe = pd.DataFrame(data=data)
        super().__init__(
            dataframe,
            name=name,
            source=source,
            context=context,
        )

    @staticmethod
    def get_item_type():
        return pd.Series

    @classmethod
    def is_valid_item(cls, item):
        return isinstance(item, cls.get_item_type())

    @staticmethod
    def is_in_memory():
        return True

    def get_dataframe(self, columns=None) -> pd.DataFrame:
        if columns:
            return self.get_data()[columns]
        else:
            return self.get_data()

    def get_count(self) -> Optional[int]:
        return self.get_dat().shape[0]

    def get_items(self):
        yield from self.get_dataframe().iterrows()

    def get_records(self):
        return self.get_mapped_items(lambda i: i[1].to_dict())

    def get_rows(self):
        return self.get_mapped_items(lambda i: i[1].to_list())

    def get_columns(self):
        return self.get_dataframe().columns

    def get_expected_count(self) -> int:
        return self.get_dataframe().shape[0]

    def take(self, count):
        return self.stream(
            self.get_dataframe().head(count),
        )

    def get_one_column_values(self, column):
        return self.get_dataframe()[column]

    def add_dataframe(self, dataframe, before=False) -> sm.ColumnarMixin:
        if before:
            frames = [dataframe, self.get_dataframe()]
        else:
            frames = [self.get_dataframe(), dataframe]
        concatenated = pd.concat(frames)
        return PandasStream(concatenated)

    def add_items(self, items, before=False) -> sm.ColumnarMixin:
        dataframe = pd.DataFrame(items)
        return self.add_dataframe(dataframe, before)

    def add_stream(self, stream, before=False) -> sm.ColumnarMixin:
        if isinstance(stream, PandasStream):
            return self.add_dataframe(stream.data, before)
        else:
            return self.add_items(stream.get_items(), before)

    def add(self, dataframe_or_stream_or_items, before=False, **kwargs) -> sm.ColumnarMixin:
        assert not kwargs
        if isinstance(dataframe_or_stream_or_items, pd.DataFrame):
            return self.add_dataframe(dataframe_or_stream_or_items, before)
        elif sm.is_stream(dataframe_or_stream_or_items):
            return self.add_stream(dataframe_or_stream_or_items, before)
        else:
            return self.add_items(dataframe_or_stream_or_items)

    def select(self, *fields, **expressions) -> sm.ColumnarMixin:
        assert not expressions, 'custom expressions are not implemented yet'
        dataframe = self.get_dataframe(columns=fields)
        return PandasStream(dataframe)

    def filter(self, *filters, **expressions) -> sm.ColumnarMixin:
        assert not filters, 'custom filters are not implemented yet'
        pandas_filter = None
        for k, v in expressions.items():
            one_filter = self.get_one_column_values(k) == v
            if pandas_filter:
                pandas_filter = pandas_filter & one_filter
            else:
                pandas_filter = one_filter
        if pandas_filter:
            return PandasStream(
                self.get_data()[pandas_filter],
                **self.get_meta()
            )
        else:
            return self

    def sort(self, *keys, reverse=False):
        dataframe = self.get_dataframe().sort_values(
            by=keys,
            ascending=not reverse,
        )
        return PandasStream(dataframe)

    def group_by(self, *keys, as_pairs=False):
        grouped = self.get_dataframe().groupby(
            by=keys,
            as_index=as_pairs,
        )
        return PandasStream(grouped)
