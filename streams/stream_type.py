from enum import Enum
import inspect

MAX_ITEMS_IN_MEMORY = 5000000
TMP_FILES_TEMPLATE = 'stream_{}.tmp'
TMP_FILES_ENCODING = 'utf8'

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg

DICT_METHOD_SUFFIX = dict(
    AnyStream='any_stream',
    LineStream='line_stream',
    RowStream='row_stream',
    KeyValueStream='key_value_stream',
    SchemaStream='schema_stream',
    RecordStream='record_stream',
    PandasStream='pandas_stream',
)


class StreamType(Enum):
    AnyStream = 'AnyStream'
    LineStream = 'LineStream'
    RowStream = 'RowStream'
    KeyValueStream = 'KeyValueStream'
    SchemaStream = 'SchemaStream'
    RecordStream = 'RecordStream'
    PandasStream = 'PandasStream'

    def get_value(self):
        return self.value

    def get_name(self):
        return self.get_value()

    def get_method_suffix(self):
        global DICT_METHOD_SUFFIX
        return DICT_METHOD_SUFFIX.get(self.get_name())

    def stream(self, data, source=arg.DEFAULT, context=arg.DEFAULT, *args, **kwargs):
        if arg.is_defined(source):
            kwargs['source'] = source
        if arg.is_defined(context):
            kwargs['context'] = context
        stream_class = self.get_class()
        return stream_class(data, *args, **kwargs)

    @staticmethod
    def detect(obj):
        if inspect.isclass(obj):
            name = obj.__name__
        else:
            name = obj.__class__.__name__
            if name == 'ItemType':
                if obj.value == 'SchemaRow':
                    return StreamType.SchemaStream
                else:
                    return StreamType('{}Stream'.format(name))
        return StreamType(name)

    @classmethod
    def of(cls, obj):
        if isinstance(obj, StreamType):
            return obj
        elif isinstance(obj, str):
            return StreamType(obj)
        else:
            return cls.detect(obj)

    def isinstance(self, stream):
        if hasattr(stream, 'get_stream_type'):
            return stream.get_stream_type() == self
