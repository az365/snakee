from typing import Optional, Callable

try:  # Assume we're a submodule in a package.
    from interfaces import Name, Options, ItemType, Stream, ConnectorInterface, Context
    from connectors.operations.abstract_sync import AbstractSync, SRC_ID, DST_ID
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Name, Options, ItemType, Stream, ConnectorInterface, Context
    from .abstract_sync import AbstractSync, SRC_ID, DST_ID


class MultiSync(AbstractSync):
    def __init__(
            self,
            name: Name,
            inputs: dict,
            outputs: dict,
            intermediates: Optional[dict],
            procedure: Optional[Callable],
            options: Optional[dict] = None,
            apply_to_stream: bool = True,
            item_type: ItemType = ItemType.Auto,
            context: Context = None,
    ):
        connectors = dict()
        for c in inputs, outputs, intermediates:
            connectors.update(c)
        self._inputs = inputs
        self._outputs = outputs
        self._intermediates = intermediates
        super().__init__(
            name=name,
            connectors=connectors,
            procedure=procedure,
            options=options,
            apply_to_stream=apply_to_stream,
            item_type=item_type,
            context=context,
        )

    def get_inputs(self) -> dict:
        return self._inputs

    def get_outputs(self) -> dict:
        return self._outputs

    def get_intermediates(self) -> dict:
        return self._intermediates

    def run_now(
            self,
            return_stream: bool = True,
            item_type: ItemType = ItemType.Auto,
            options: Options = None,
            verbose: bool = True,
    ) -> Stream:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        stream = self.get_src().to_stream(item_type=item_type)
        if verbose:
            self.log(f'Running operation: {self.get_name()}')
        if self.has_procedure():
            if self._apply_to_stream:
                stream = self.get_procedure()(stream, **self.get_kwargs(ex=SRC_ID, upd=options))
            else:
                stream = self.get_procedure()(**self.get_kwargs())
        return stream.write_to(self.get_dst(), return_stream=return_stream)

    def run_if_not_yet(
            self,
            raise_error_if_exists: bool = False,
            return_stream: bool = True,
            item_type: ItemType = ItemType.Auto,
            options: Options = None,
            verbose: bool = True,
    ) -> Optional[Stream]:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        if not self.is_done():
            return self.run_now(return_stream=return_stream, item_type=item_type, verbose=verbose)
        elif raise_error_if_exists:
            raise ValueError(f'object {self.get_dst()} already exists')
        elif return_stream:
            if verbose:
                self.log(f'Operation is already done: {self.get_name()}')
            return self.get_dst().to_stream(item_type=item_type)

    def to_stream(self, item_type: ItemType = ItemType.Auto, **kwargs):
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        return self.run_if_not_yet(raise_error_if_exists=False, return_stream=True, item_type=item_type)

    def from_stream(self, stream: Stream, rewrite: bool = False) -> Optional[ConnectorInterface]:
        if rewrite or not self.has_inputs():
            self.get_src().from_stream(stream)
        else:
            self.log(f'src-object ({self.get_src()}) is already exists')
        if rewrite or not self.has_outputs():
            return self.get_dst().from_stream(stream)
        else:
            self.log(f'dst-object ({self.get_dst()}) is already exists')

    @staticmethod
    def _assume_connector(connector) -> ConnectorInterface:
        return connector
