from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.sync.operation import (
        Operation,
        Name, Stream, Connector, Context, Options, OptStreamType, StreamType,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .operation import (
        Operation,
        Name, Stream, Connector, Context, Options, OptStreamType, StreamType,
    )

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
            stream_type: OptStreamType = arg.DEFAULT,
            context: Context = arg.DEFAULT,
    ):
        super().__init__(
            name=name,
            connectors=connectors,
            procedure=procedure,
            context=context,
        )
        stream_type = arg.undefault(stream_type, StreamType.RecordStream)
        assert isinstance(stream_type, StreamType)
        self._stream_type = stream_type
        self._apply_to_stream = apply_to_stream
        self._options = options

    def get_stream_type(self) -> StreamType:
        return self._stream_type

    def get_src(self) -> Connector:
        conn = self.get_child(SRC_ID)
        return self._assume_connector(conn)

    def get_dst(self) -> Connector:
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

    def get_stream(self, run_if_not_yet: bool = False, stream_type: OptStreamType = arg.DEFAULT) -> Stream:
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        if run_if_not_yet and not self.is_done():
            self.run_now()
        return self.get_dst().to_stream(stream_type=stream_type)

    def to_stream(self, stream_type: Union[StreamType, arg.DefaultArgument] = arg.DEFAULT):
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        return self.run_if_not_yet(raise_error_if_exists=False, return_stream=True, stream_type=stream_type)

    @abstractmethod
    def run_now(
            self,
            return_stream: bool = True,
            stream_type: OptStreamType = arg.DEFAULT,
            options: Union[dict, arg.DefaultArgument, None] = None,
            verbose: bool = True,
    ) -> Optional[Stream]:
        pass

    def run_if_not_yet(
            self,
            raise_error_if_exists: bool = False,
            return_stream: bool = True,
            stream_type: Union[StreamType, arg.DefaultArgument] = arg.DEFAULT,
            options: Options = None,
            verbose: bool = True,
    ) -> Optional[Stream]:
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        if not self.is_done():
            return self.run_now(return_stream=return_stream, stream_type=stream_type, options=options, verbose=verbose)
        elif raise_error_if_exists:
            raise ValueError('object(s) {} already exists'.format(', '.join(self.get_existing_outputs())))
        elif return_stream:
            if verbose:
                self.log('Operation is already done: {}'.format(self.get_name()))
            return self.get_stream(stream_type=stream_type)

    @staticmethod
    def _assume_connector(connector) -> Connector:
        return connector
