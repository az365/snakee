from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from streams import stream_classes as sm
    from utils import arguments as arg


class ColumnarMixin(sm.AbstractStream, ABC):

    @abstractmethod
    def get_item_type(self):
        pass

    @classmethod
    def is_valid_item(cls, item):
        return cls.get_item_type().isinstance(item)

    @classmethod
    def get_validated(cls, items, skip_errors=False, context=None):
        for i in items:
            if cls.is_valid_item(i):
                yield i
            else:
                message = 'get_validated(): item {} is not a {}'.format(i, cls.get_item_type())
                if skip_errors:
                    if context:
                        context.get_logger().log(message)
                else:
                    raise TypeError(message)

    @abstractmethod
    def get_items(self):
        pass

    def validated(self, skip_errors=False):
        return self.__class__(
            self.get_validated(self.get_items(), skip_errors=skip_errors),
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

    def get_demo_example(self, count=10, filters=[], columns=None):
        sm_sample = self.filter(*filters) if filters else self
        return sm_sample.take(count).get_dataframe(columns)

    def show(self, count=10, filters=[], columns=None):
        self.log(self.get_description(), force=True)
        self.get_demo_example(count=count, filters=filters, columns=columns)
