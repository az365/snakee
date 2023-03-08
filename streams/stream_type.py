from inspect import isclass

try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.classes.enum import ClassType

DICT_METHOD_SUFFIX = dict(
    AnyStream='any_stream',
    LineStream='line_stream',
    RowStream='row_stream',
    KeyValueStream='key_value_stream',
    StructStream='struct_stream',
    RecordStream='record_stream',
    PandasStream='pandas_stream',
    SqlStream='sql_stream',
)


class StreamType(ClassType):
    AnyStream = 'AnyStream'
    LineStream = 'LineStream'
    RowStream = 'RowStream'
    KeyValueStream = 'KeyValueStream'
    StructStream = 'StructStream'
    RecordStream = 'RecordStream'
    PandasStream = 'PandasStream'
    SqlStream = 'SqlStream'

    def get_method_suffix(self):
        global DICT_METHOD_SUFFIX
        return DICT_METHOD_SUFFIX.get(self.get_name())

    def stream(self, data, *args, **kwargs):
        stream_class = self.get_class()
        return stream_class(data, *args, **kwargs)

    @classmethod
    def detect(cls, obj, default=None) -> ClassType:
        if isinstance(obj, StreamType):
            return obj
        elif isinstance(obj, str):
            name = obj
        elif isclass(obj):
            name = obj.__name__
        else:
            name = obj.__class__.__name__
            if name == 'ItemType':
                item_type_name = obj.get_name()
                if item_type_name == 'StructRow':
                    stream_type_obj = StreamType.StructStream
                else:
                    stream_type_name = f'{item_type_name}Stream'
                    stream_type_obj = cls.find_instance(stream_type_name)
                if stream_type_obj is None:
                    if default is not None:
                        stream_type_obj = default
                    else:
                        stream_type_obj = cls.get_default()
                return stream_type_obj
        return StreamType(name)

    @classmethod
    def of(cls, obj):
        if isinstance(obj, StreamType):
            return obj
        elif isinstance(obj, str):
            return StreamType(obj)
        else:
            return cls.detect(obj)

    def get_item_type(self):
        stream_class = self.get_class()
        if hasattr(stream_class, 'get_default_item_type'):
            return stream_class.get_default_item_type()

    def get_stream_class(self):
        return self.get_class()

    def isinstance(self, stream, by_type: bool = True) -> bool:
        if by_type and hasattr(stream, 'get_stream_type'):
            return stream.get_stream_type() == self
        else:
            return super().isinstance(stream, by_type=by_type)


StreamType.prepare()
