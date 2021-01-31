try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from connectors import (
        abstract_connector as ac,
        connector_classes as cs,
    )
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams import stream_classes as sm
    from .. import (
        connector_classes as cs,
        abstract_connector as ac,
    )
    from ...utils import arguments as arg


class TwinSync(ac.LeafConnector):
    def __init__(
            self,
            name,
            src,
            dst,
            stream_type=sm.StreamType.AnyStream,
            procedure=None,
            apply_to_stream=True,
            context=arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            parent=context,
        )
        assert isinstance(src, ac.LeafConnector)
        self.src = src
        assert isinstance(dst, ac.LeafConnector)
        self.dst = dst
        assert procedure is None or isinstance(procedure, callable)
        self.procedure = procedure
        assert isinstance(stream_type, sm.StreamType)
        self.stream_type = stream_type
        self.apply_to_stream = apply_to_stream

    def is_existing(self):
        if self.dst.is_existing():
            return True
        elif self.src.is_existing():
            return True
        return False

    def run_now(self, return_stream=True, stream_type=arg.DEFAULT):
        stream_type = arg.undefault(stream_type, self.stream_type)
        stream = self.src.to_stream(stream_type)
        if self.procedure:
            if self.apply_to_stream:
                stream = self.procedure(stream)
            else:
                stream = stream.apply(self.procedure)
        return stream.save_to(self.dst, return_stream=return_stream)

    def run_if_not_yet(self, raise_error_if_exists=False, return_stream=True, stream_type=arg.DEFAULT):
        stream_type = arg.undefault(stream_type, self.stream_type)
        if not self.dst.is_existing():
            return self.run_now(return_stream=return_stream, stream_type=stream_type)
        elif raise_error_if_exists:
            raise ValueError('object already exists')
        elif return_stream:
            return self.dst.to_stream(stream_type)

    def to_stream(self, stream_type=arg.DEFAULT):
        stream_type = arg.undefault(stream_type, self.stream_type)
        self.stream_type = stream_type
        return self.run_if_not_yet(raise_error_if_exists=False, return_stream=True, stream_type=stream_type)

    def from_stream(self, stream, rewrite=False):
        if rewrite or not self.src.is_existing():
            self.src.from_stream(stream)
        else:
            self.log('src-object ({}) is already exists'.format(self.src.get_path()))
        if rewrite or not self.dst.is_existing():
            self.dst.from_stream(stream)
        else:
            self.log('dst-object ({}) is already exists'.format(self.dst.get_path()))
