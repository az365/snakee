from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ContextInterface, StreamInterface, Stream, Connector,
        AUTO, Auto, AutoContext, StreamItemType, StreamType, Name,
    )
    from connectors import connector_classes as ct
    from connectors.operations.abstract_sync import AbstractSync, SRC_ID, DST_ID
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ContextInterface, StreamInterface, Stream, Connector,
        AUTO, Auto, AutoContext, StreamItemType, StreamType, Name,
    )
    from .. import connector_classes as ct
    from .abstract_sync import AbstractSync, SRC_ID, DST_ID

Options = Union[dict, Auto, None]


class TwinSync(AbstractSync):
    def __init__(
            self,
            name: Name,
            src: Connector,
            dst: Connector,
            procedure: Optional[Callable],
            options: Options = None,
            apply_to_stream: bool = True,
            stream_type: StreamItemType = AUTO,
            context: AutoContext = AUTO,
    ):
        if not Auto.is_defined(options):
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
        if Auto.is_defined(upd):
            kwargs.update(upd)
        return kwargs

    def run_now(
            self,
            return_stream: bool = True,
            stream_type: StreamItemType = AUTO,
            options: Options = None,
            verbose: bool = True,
    ) -> Stream:
        stream_type = Auto.delayed_acquire(stream_type, self.get_stream_type)
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
