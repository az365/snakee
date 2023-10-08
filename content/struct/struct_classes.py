try:  # Assume we're a submodule in a package.
    from content.struct.struct_type import *
    from content.struct.flat_struct import *
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .struct_type import StructType
    from .flat_struct import FlatStruct

DEFAULT_STREAM_CLASS = FlatStruct
DICT_STRUCT_CLASSES = dict(
    FlatStruct=FlatStruct,
)

StructType.set_default(DEFAULT_STREAM_CLASS.__name__)
StructType.set_dict_classes(DICT_STRUCT_CLASSES)
