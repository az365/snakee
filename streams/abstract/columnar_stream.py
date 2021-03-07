from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import arguments as arg


class ColumnarStream(sm.AbstractStream, ABC):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            source=None,
            context=None,
            check=True,
    ):
        super().__init__(
            data=data,
            name=name,
            source=source,
            context=context,
        )
        self.check = check

    @abstractmethod
    def get_item_type(self):
        pass

    @staticmethod
    @abstractmethod
    def is_valid_item(item):
        pass

    @staticmethod
    def get_valid_items(items, **kwargs):
        pass

    @abstractmethod
    def get_items(self):
        pass

    def validated(self, skip_errors=False):
        return self.__class__(
            self.get_valid_items(self.get_items(), skip_errors=skip_errors),
            **self.get_meta()
        )

    def get_shape(self):
        return self.get_expected_count(), self.get_column_count()

    def get_description(self):
        return '{} rows, {} columns: {}'.format(
            self.get_str_count(),
            self.get_column_count(),
            ', '.join(self.get_columns()),
        )

    def get_column_count(self):
        return len(self.get_columns())

    @abstractmethod
    def get_expected_count(self):
        pass

    def get_estimated_count(self):
        return self.get_expected_count()

    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_one_column(self, column):
        pass

    @abstractmethod
    def select(self, *fields, **expressions):
        pass

    @abstractmethod
    def filter(self, *filters, **expressions):
        pass

    @abstractmethod
    def sort(self, *keys, reverse=False):
        pass

    @abstractmethod
    def group_by(self, *keys):
        pass

    @abstractmethod
    def is_in_memory(self):
        pass

    @abstractmethod
    def get_records(self):
        pass

    @abstractmethod
    def get_dataframe(self, columns=None):
        pass

    def to_records(self, **kwargs):
        return sm.RecordStream(
            self.get_records(),
            count=self.get_expected_count(),
        )

    def to_rows(self, *columns, **kwargs):
        return self.select(
            columns,
        ).to_records()

    # def get_description(self):
    #     if self.can_be_in_memory():
    #         self.data = self.get_list()
    #         self.count = self.get_count()
    #     return [self.get_expected_count(), self.get_columns()]

    def get_demo_example(self, count=10, filters=[], columns=None):
        sm_sample = self.filter(*filters) if filters else self
        return sm_sample.take(count).get_dataframe(columns)

    def show(self, count=10, filters=[], columns=None):
        self.log(self.get_description(), force=True)
        self.get_demo_example(count=count, filters=filters, columns=columns)
