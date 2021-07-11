from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors import connector_classes as ct
    from connectors.sync.abstract_sync import AbstractSync, SRC_ID, DST_ID
    from base.interfaces.context_interface import ContextInterface
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.stream_type import StreamType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .. import connector_classes as ct
    from .abstract_sync import AbstractSync, SRC_ID, DST_ID
    from ...base.interfaces.context_interface import ContextInterface
    from ...streams.interfaces.abstract_stream_interface import StreamInterface
    from ...streams.stream_type import StreamType

Name = str
Stream = StreamInterface
OptStreamType = Union[StreamType, arg.DefaultArgument]
Options = Union[dict, arg.DefaultArgument, None]
Connector = ct.LeafConnector
Context = Union[ContextInterface, arg.DefaultArgument, None]


class TwinSync(AbstractSync):
    def __init__(
            self,
            name: Name,
            src: Connector,
            dst: Connector,
            procedure: Optional[Callable],
            options: Options = None,
            apply_to_stream: bool = True,
            stream_type: OptStreamType = arg.DEFAULT,
            context: Context = arg.DEFAULT,
    ):
        if not arg.is_defined(options):
            options = dict()
        super().__init__(
            name=name,
            connectors={SRC_ID: src, DST_ID: dst},
            procedure=procedure,
            options=options,
            apply_to_stream=apply_to_stream,
            stream_type=stream_type,
            context=context,
        )

    def get_inputs(self) -> dict:
        return {SRC_ID: self.get_src()}

    def get_outputs(self) -> dict:
        return {DST_ID: self.get_dst()}

    def get_intermediates(self) -> dict:
        return dict()

    def has_apply_to_stream(self) -> bool:
        return self._apply_to_stream

    def get_kwargs(self, ex: Optional[Iterable] = None, upd: Optional[dict] = None) -> dict:
        kwargs = self.get_options().copy()
        if ex:
            for k in ex:
                kwargs.pop(k)
        if arg.is_defined(upd):
            kwargs.update(upd)
        return kwargs

    def run_now(
            self,
            return_stream: bool = True,
            stream_type: OptStreamType = arg.DEFAULT,
            options: Options = None,
            verbose: bool = True,
    ) -> Stream:
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        stream = self.get_src().to_stream(stream_type=stream_type)
        if verbose:
            self.log('Running operation: {}'.format(self.get_name()))
        if self.has_procedure():
            if self.has_apply_to_stream():
                stream = self.get_procedure()(stream, **self.get_kwargs(upd=options))
            else:
                stream = stream.apply_to_data(self.get_procedure(), **self.get_kwargs(upd=options))
        return stream.write_to(self.get_dst(), return_stream=return_stream)

    def from_stream(self, stream: Stream, rewrite: bool = False) -> Optional[Connector]:
        if rewrite or not self.has_inputs():
            self.get_src().from_stream(stream)
        else:
            self.log('src-object ({}) is already exists'.format(self.get_src()))
        if rewrite or not self.has_outputs():
            return self.get_dst().from_stream(stream)
        else:
            self.log('dst-object ({}) is already exists'.format(self.get_dst()))

    @staticmethod
    def _assume_connector(connector) -> Connector:
        return connector
