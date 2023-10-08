from typing import Optional, Iterable, Callable

try:  # Assume we're a submodule in a package.
    from interfaces import Context, StreamInterface, Stream, Connector, ItemType, Name
    from connectors import connector_classes as ct
    from connectors.operations.abstract_sync import AbstractSync, SRC_ID, DST_ID
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Context, StreamInterface, Stream, Connector, ItemType, Name
    from .. import connector_classes as ct
    from .abstract_sync import AbstractSync, SRC_ID, DST_ID


class TwinSync(AbstractSync):
    def __init__(
            self,
            name: Name,
            src: Connector,
            dst: Connector,
            procedure: Optional[Callable],
            options: Optional[dict] = None,
            apply_to_stream: bool = True,
            item_type: ItemType = ItemType.Auto,
            context: Context = None,
    ):
        if options is None:
            options = dict()
        super().__init__(
            name=name,
            connectors={SRC_ID: src, DST_ID: dst},
            procedure=procedure,
            options=options,
            apply_to_stream=apply_to_stream,
            item_type=item_type,
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
        if upd is not None:
            kwargs.update(upd)
        return kwargs

    def run_now(
            self,
            return_stream: bool = True,
            item_type: ItemType = ItemType.Auto,
            options: Optional[dict] = None,
            verbose: bool = True,
    ) -> Stream:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        stream = self.get_src().to_stream(item_type=item_type)
        if verbose:
            self.log(f'Running operation: {self.get_name()}')
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
            src = self.get_src()
            self.log(f'src-object ({src}) is already exists')
        if rewrite or not self.has_outputs():
            return self.get_dst().from_stream(stream)
        else:
            dst = self.get_dst()
            self.log(f'dst-object ({dst}) is already exists')

    @staticmethod
    def _assume_connector(connector) -> Connector:
        return connector
