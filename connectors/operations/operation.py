from typing import Optional, Iterable, Callable, Any

try:  # Assume we're a submodule in a package.
    from interfaces import Context, Name, Options
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Context, Name, Options
    from .. import connector_classes as ct


class Operation(ct.HierarchicConnector):
    def __init__(
            self,
            name: Name,
            connectors: dict,
            procedure: Optional[Callable],
            options: Optional[dict] = None,
            context: Context = None,
    ):
        if context is None:
            context = ct.get_context()
        super().__init__(name=name, parent=context, children=connectors)
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

    def check(self) -> None:
        for conn in self.get_connectors():
            conn.check()

    def get_kwargs(self, ex: Optional[Iterable] = None, upd: Options = None) -> dict:
        kwargs = dict()
        kwargs.update(self.get_connectors())
        kwargs.update(self.get_options())
        if ex is not None:
            for k in ex:
                kwargs.pop(k)
        if upd is not None:
            kwargs.update(upd)
        return kwargs

    def run_now(self, options: Optional[dict] = None) -> Any:
        assert self.has_procedure()
        return self.get_procedure()(**self.get_kwargs(upd=options))
