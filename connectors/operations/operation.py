from typing import Optional, Iterable, Callable, Union, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.context_interface import ContextInterface
    from connectors.abstract.connector_interface import ConnectorInterface
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.stream_type import StreamType
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.context_interface import ContextInterface
    from ..abstract.connector_interface import ConnectorInterface
    from ...streams.interfaces.abstract_stream_interface import StreamInterface
    from ...streams.stream_type import StreamType
    from .. import connector_classes as ct

Name = str
Stream = StreamInterface
OptStreamType = Union[StreamType, arg.DefaultArgument]
Connector = ConnectorInterface
Context = Union[ContextInterface, arg.DefaultArgument, None]
Options = Union[dict, arg.DefaultArgument, None]


class Operation(ct.HierarchicConnector):
    def __init__(
            self,
            name: Name,
            connectors: dict,
            procedure: Optional[Callable],
            options: Optional[dict] = None,
            context: Context = arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            children=connectors,
            parent=arg.undefault(context, ct.get_context()),
        )
        assert procedure is None or isinstance(procedure, Callable)
        self._procedure = procedure
        self._options = options or dict()

    @staticmethod
    def is_storage() -> bool:
        return False

    @staticmethod
    def is_folder() -> bool:
        return False

    @staticmethod
    def get_default_child_class():
        return ct.LeafConnector

    def get_connectors(self) -> dict:
        return self.get_children()

    def get_options(self) -> dict:
        return self._options

    def get_procedure(self) -> Optional[Callable]:
        return self._procedure

    def has_procedure(self) -> bool:
        return isinstance(self.get_procedure(), Callable)

    def check(self) -> NoReturn:
        for conn in self.get_connectors():
            conn.check()

    def get_kwargs(self, ex: Optional[Iterable] = None, upd: Options = None) -> dict:
        kwargs = dict()
        kwargs.update(self.get_connectors())
        kwargs.update(self.get_options())
        if arg.is_defined(ex):
            for k in ex:
                kwargs.pop(k)
        if arg.is_defined(upd):
            kwargs.update(upd)
        return kwargs

    def run_now(self, options: Optional[dict] = None) -> Any:
        assert self.has_procedure()
        return self.get_procedure()(**self.get_kwargs(upd=options))
