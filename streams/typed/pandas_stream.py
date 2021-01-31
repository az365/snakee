import pandas as pd

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as fx
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as fx
    from utils import arguments as arg


class PandasStream(fx.RecordStream):
    def __init__(
            self,
            data,
            count=None,
            less_than=None,
            check=False,
            source=None,
            context=None,
            max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=fx.TMP_FILES_TEMPLATE,
            tmp_files_encoding=fx.TMP_FILES_ENCODING,
    ):
        if isinstance(data, pd.DataFrame):
            dataframe = data
        elif isinstance(data, fx.RecordStream):
            dataframe = data.get_dataframe()
        else:  # isinstance(data, (list, tuple)):
            dataframe = pd.DataFrame(data=data)
        super().__init__(
            dataframe,
            count=count or dataframe.shape[1],
            less_than=less_than or dataframe.shape[1],
            check=check,
            source=source,
            context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )

    def iterable(self, as_records=True):
        for i in self.get_dataframe().iterrows():
            if as_records:
                yield i[1].to_dict()
            else:
                yield i[1]

    def expected_count(self):
        return self.data.shape[0]

    def final_count(self):
        return self.data.shape[0]

    def get_records(self, **kwargs):
        yield from self.iterable(as_records=True)

    def get_one_column(self, column):
        return self.data[column]

    def get_dataframe(self, columns=None):
        if columns:
            return self.data[columns]
        else:
            return self.data

    def add_dataframe(self, dataframe, before=False):
        if before:
            frames = [dataframe, self.data]
        else:
            frames = [self.data, dataframe]
        concatenated = pd.concat(frames)
        return PandasStream(concatenated)

    def add_items(self, items, before=False):
        dataframe = pd.DataFrame(items)
        return self.add_dataframe(dataframe, before)

    def add_stream(self, stream, before=False):
        if isinstance(stream, PandasStream):
            return self.add_dataframe(stream.data, before)
        else:
            return self.add_items(stream.get_items(), before)

    def add(self, dataframe_or_stream_or_items, before=False, **kwargs):
        assert not kwargs
        if isinstance(dataframe_or_stream_or_items, pd.DataFrame):
            return self.add_dataframe(dataframe_or_stream_or_items, before)
        elif fx.is_stream(dataframe_or_stream_or_items):
            return self.add_stream(dataframe_or_stream_or_items, before)
        else:
            return self.add_items(dataframe_or_stream_or_items)

    def select(self, *fields, **expressions):
        assert not expressions, 'custom expressions are not implemented yet'
        dataframe = self.get_dataframe(columns=fields)
        return PandasStream(dataframe)

    def filter(self, *filters, **expressions):
        assert not filters, 'custom filters are not implemented yet'
        pandas_filter = None
        for k, v in expressions.items():
            one_filter = self.get_one_column(k) == v
            if pandas_filter:
                pandas_filter = pandas_filter & one_filter
            else:
                pandas_filter = one_filter
        if pandas_filter:
            return PandasStream(
                self.data[pandas_filter],
                **self.get_meta()
            )
        else:
            return self

    def sort(self, *keys, reverse=False, step=arg.DEFAULT, verbose=True):
        dataframe = self.get_dataframe().sort_values(
            by=keys,
            ascending=not reverse,
        )
        return PandasStream(dataframe)

    def group_by(self, *keys, step=arg.DEFAULT, as_pairs=True, verbose=True):
        grouped = self.get_dataframe().groupby(
            by=keys,
            as_index=as_pairs,
        )
        return PandasStream(grouped)

    def is_in_memory(self):
        return True

    def to_memory(self):
        pass

    def to_records(self, **kwargs):
        return fx.RecordStream(
            self.get_records(),
            count=self.expected_count(),
        )

    def to_rows(self, *columns, **kwargs):
        return self.select(
            columns,
        ).to_records()

    def show(self, count=10, filters={}):
        self.log(['Show:', self.class_name(), self.get_meta(), '\n'])
        return self.filter(**filters).get_dataframe().head(count)
