try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import arguments as arg


class KeyValueStream(sm.RowStream):
    def __init__(
            self,
            data,
            name=arg.DEFAULT, check=True,
            count=None, less_than=None,
            value_stream_type=None,
            source=None, context=None,
            max_items_in_memory=arg.DEFAULT,
            tmp_files_template=arg.DEFAULT,
            tmp_files_encoding=arg.DEFAULT,
    ):
        super().__init__(
            data,
            name=name, check=check,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        if value_stream_type is None:
            self.value_stream_type = sm.StreamType.AnyStream
        else:
            assert value_stream_type in sm.StreamType
            self.value_stream_type = value_stream_type or sm.StreamType.AnyStream

    @staticmethod
    def is_valid_item(item):
        if isinstance(item, (list, tuple)):
            return len(item) == 2

    @staticmethod
    def get_key(item):
        return item[0]

    @staticmethod
    def get_value(item):
        return item[1]

    def get_keys(self):
        keys = self.get_mapped_items(self.get_key(i))
        return list(keys) if self.is_in_memory() else keys

    def get_values(self):
        values = self.get_mapped_items(self.get_value(i))
        return list(values) if self.is_in_memory() else values

    def get_value_stream_type(self):
        return self.value_stream_type

    def values(self):
        return self.stream(
            self.get_values(),
            stream_type=self.get_value_stream_type()
        )

    def keys(self, uniq, stream_type=arg.DEFAULT):
        return self.stream(
            self.get_uniq_keys() if uniq else self.get_keys(),
            stream_type=arg.undefault(stream_type, sm.StreamType.AnyStream),
        )

    def get_uniq_keys(self):
        my_keys = list()
        for i in self.get_items():
            key = self.get_key(i)
            if key in my_keys:
                pass
            else:
                yield key
        return my_keys

    def extract_keys_in_memory(self):
        stream_for_keys, stream_for_items = self.get_tee(2)
        return (
            stream_for_keys.keys(),
            stream_for_items,
        )

    def extract_keys(self):
        if self.is_in_memory():
            return self.extract_keys_in_memory()
        else:
            if hasattr(self, 'extract_keys_on_disk'):
                return self.extract_keys_on_disk()

    def memory_sort_by_key(self, reverse=False):
        return self.memory_sort(
            key=self.get_key,
            reverse=reverse,
        )

    def disk_sort_by_key(self, reverse=False, step=arg.DEFAULT):
        step = arg.undefault(step, self.max_items_in_memory)
        return self.disk_sort(
            key=self.get_key,
            reverse=reverse,
            step=step,
        )

    def sorted_group_by_key(self):
        def get_groups():
            accumulated = list()
            prev_k = None
            for k, v in self.get_data():
                if (k != prev_k) and accumulated:
                    yield prev_k, accumulated
                    accumulated = list()
                prev_k = k
                accumulated.append(v)
            yield prev_k, accumulated
        sm_groups = self.stream(
            get_groups(),
        )
        if self.is_in_memory():
            sm_groups = sm_groups.to_memory()
        return sm_groups

    def get_dict(self, of_lists=False):
        result = dict()
        if of_lists:
            for k, v in self.get_items():
                distinct = result.get(k, [])
                if v not in distinct:
                    result[k] = distinct + [v]
        else:
            for k, v in self.get_items():
                result[k] = v
        return result
