from typing import Optional, Iterable, Union, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.context_interface import ContextInterface
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.sync.operation import Operation
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.interfaces.context_interface import ContextInterface
    from ..abstract.hierarchic_connector import HierarchicConnector
    from .operation import Operation
    from .. import connector_classes as ct

Name = str
Native = HierarchicConnector
OptContext = Union[ContextInterface, arg.DefaultArgument]


class Job(HierarchicConnector):
    def __init__(
            self,
            name: Name,
            operations: Optional[dict] = None,
            queue: Optional[list] = None,
            options: Optional[dict] = None,
            context: OptContext = arg.DEFAULT,
    ):
        context = arg.undefault(context, ct.get_context())
        super().__init__(
            name=name,
            children=operations,
            parent=context,
        )
        self._queue = queue or list()
        self._options = options or dict()

    @staticmethod
    def get_default_child_class():
        return Operation

    @staticmethod
    def is_storage() -> bool:
        return False

    @staticmethod
    def is_folder() -> bool:
        return False

    def get_queue(self) -> list:
        return self._queue

    def set_queue(self, queue: list):
        self._queue = queue

    def add_operation(self, operation: Operation, inplace: bool = False, add_to_queue: bool = True) -> Optional[Native]:
        if add_to_queue:
            self.get_queue().append(operation)
        return self.add_child(operation, check=False, inplace=inplace)

    def get_operation(self, operation: Union[Name, Operation]) -> Operation:
        if isinstance(operation, Name):
            operation = self.get_operations()[operation]
        return operation

    def set_operations(self, operations: Iterable) -> Native:
        if not isinstance(operations, dict):
            operations = {op.get_name(): op for op in operations}
        return self.set_data(operations, inplace=False)

    def get_operations(self) -> dict:
        return self.get_children()

    def get_options(self, including: Union[Operation, Iterable, None] = None, upd: Optional[dict] = None) -> dict:
        defined_options = self._options.copy()
        if arg.is_defined(upd):
            defined_options.update(upd)
        if including:
            if hasattr(including, 'get_options'):
                including = including.get_options()
            assert isinstance(including, Iterable)
            options = dict()
            for name in including:
                if name in defined_options:
                    options[name] = self._options[name]
        else:
            options = defined_options
        return options

    def get_connectors(self) -> dict:
        connectors = dict()
        for _, op in self.get_operations().items():
            assert isinstance(op, Operation)
            connectors.update(op.get_connectors())
        return connectors

    def get_inputs(self) -> dict:
        inputs = dict()
        for _, op in self.get_operations().items():
            if hasattr(op, 'get_inputs'):
                inputs.update(op.get_inputs())
        return inputs

    def get_outputs(self) -> dict:
        outputs = dict()
        for _, op in self.get_operations().items():
            if hasattr(op, 'get_outputs'):
                outputs.update(op.get_outputs())
        return outputs

    def has_inputs(self) -> bool:
        for _, op in self.get_operations().items():
            if hasattr(op, 'has_inputs'):
                if not op.has_inputs():
                    return False
        return True

    def has_outputs(self) -> bool:
        for _, op in self.get_operations().items():
            if hasattr(op, 'has_outputs'):
                if not op.has_outputs():
                    return False
        return True

    def is_done(self) -> bool:
        return self.has_outputs()

    def check(self) -> NoReturn:
        for c in self.get_connectors():
            c.check()

    def run(
            self,
            operations: Union[list, arg.DefaultArgument] = arg.DEFAULT,
            if_not_yet: bool = True,
            options: Optional[dict] = None,
    ):
        operations = arg.undefault(operations, self.get_queue())
        operations = [self.get_operation(op) for op in operations]
        names = [op.get_name() for op in operations]
        for name, operation in zip(names, operations):
            options = self.get_options(including=operation, upd=options)
            if if_not_yet and hasattr(operation, 'run_if_not_yet'):
                operation.run_if_not_yet(options=options)
            else:
                operation.run_now(options=options)

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
