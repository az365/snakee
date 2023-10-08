from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable

try:  # Assume we're a submodule in a package.
    from interfaces import Name, Stream, ConnectorInterface, Context, Options, ItemType, StreamType
    from base.constants.chars import ITEMS_DELIMITER
    from base.functions.arguments import get_value
    from connectors.operations.operation import Operation
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Name, Stream, ConnectorInterface, Context, Options, ItemType, StreamType
    from ...base.constants.chars import ITEMS_DELIMITER
    from ...base.functions.arguments import get_value
    from ...connectors.operations.operation import Operation

SRC_ID = 'src'
DST_ID = 'dst'


class AbstractSync(Operation, ABC):
    def __init__(
            self,
            name: Name,
            connectors: dict,
            procedure: Optional[Callable],
            options: Optional[dict] = None,
            apply_to_stream: bool = True,
            item_type: ItemType = ItemType.Auto,
            context: Context = None,
    ):
        super().__init__(
            name=name,
            connectors=connectors,
            procedure=procedure,
            context=context,
        )
        if item_type in (ItemType.Auto, None):
            item_type = ItemType.Record
        if not isinstance(item_type, ItemType):
            item_type = ItemType(get_value(item_type))
        self._item_type = item_type
        self._apply_to_stream = apply_to_stream
        self._options = options

    def get_item_type(self) -> ItemType:
        return self._item_type

    def get_src(self) -> ConnectorInterface:
        conn = self.get_child(SRC_ID)
        return self._assume_connector(conn)

    def get_dst(self) -> ConnectorInterface:
        conn = self.get_child(DST_ID)
        return self._assume_connector(conn)

    @abstractmethod
    def get_inputs(self) -> dict:
        pass

    @abstractmethod
    def get_outputs(self) -> dict:
        pass

    @abstractmethod
    def get_intermediates(self) -> dict:
        pass

    def has_inputs(self) -> bool:
        for name, conn in self.get_inputs().items():
            if not conn.is_existing():
                return False
        return True

    def get_existing_outputs(self) -> Iterable:
        for name, conn in self.get_outputs().items():
            if conn.is_existing():
                yield conn

    def has_outputs(self) -> bool:
        for name, conn in self.get_outputs().items():
            if not conn.is_existing():
                return False
        return True

    def is_done(self) -> bool:
        return self.has_outputs()

    def is_existing(self) -> bool:
        return self.has_inputs() or self.has_outputs()

    def get_stream(self, run_if_not_yet: bool = False, item_type: ItemType = ItemType.Auto) -> Stream:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        if run_if_not_yet and not self.is_done():
            self.run_now()
        return self.get_dst().to_stream(item_type=item_type)

    def to_stream(self, item_type: ItemType = ItemType.Auto) -> Stream:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        return self.run_if_not_yet(raise_error_if_exists=False, return_stream=True, item_type=item_type)

    @abstractmethod
    def run_now(
            self,
            return_stream: bool = True,
            item_type: ItemType = ItemType.Auto,
            options: Options = None,
            verbose: bool = True,
    ) -> Optional[Stream]:
        pass

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
            return self.run_now(return_stream=return_stream, item_type=item_type, options=options, verbose=verbose)
        elif raise_error_if_exists:
            objects_str = ITEMS_DELIMITER.join(self.get_existing_outputs())
            raise ValueError(f'object(s) {objects_str} already exists')
        elif return_stream:
            if verbose:
                self.log(f'Operation is already done: {self.get_name()}')
            return self.get_stream(item_type=item_type)

    @staticmethod
    def _assume_connector(connector) -> ConnectorInterface:
        return connector
