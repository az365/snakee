from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        algo,
    )
    from base.interfaces.context_interface import ContextInterface
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from connectors.abstract.connector_interface import ConnectorInterface
    from connectors.filesystem.temporary_interface import TemporaryLocationInterface, TemporaryFilesMaskInterface
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        algo,
    )
    from ...base.interfaces.context_interface import ContextInterface
    from ...streams.interfaces.abstract_stream_interface import StreamInterface
    from ..abstract.connector_interface import ConnectorInterface
    from .temporary_interface import TemporaryLocationInterface, TemporaryFilesMaskInterface
    from .. import connector_classes as ct

Mask = str
Name = str
Stream = StreamInterface
Connector = ConnectorInterface
Context = Union[ContextInterface, arg.DefaultArgument]
Parent = Union[Connector, Context]
Verbose = Union[bool, arg.DefaultArgument]

DEFAULT_FOLDER = 'tmp'
DEFAULT_MASK = 'stream_{}_part{}.tmp'
PART_PLACEHOLDER = '{:03}'
DEFAULT_ENCODING = 'utf8'


class TemporaryLocation(ct.LocalFolder, TemporaryLocationInterface):
    def __init__(
            self,
            path: Name = DEFAULT_FOLDER,
            mask: Mask = DEFAULT_MASK,
            path_is_relative: bool = True,
            parent: Parent = arg.DEFAULT,
            context: Context = arg.DEFAULT,
            verbose=arg.DEFAULT,
    ):
        parent = arg.undefault(parent, ct.LocalStorage(context=context))
        super().__init__(
            path=path,
            path_is_relative=path_is_relative,
            parent=parent,
            verbose=verbose,
        )
        mask = mask.replace('*', '{}')
        assert arg.is_formatter(mask, 2)
        self._mask = mask

    def get_str_mask_template(self) -> Mask:
        return self._mask

    def mask(self, mask: Mask) -> ConnectorInterface:
        return self.stream_mask(mask)

    def stream_mask(self, stream_or_name: Union[StreamInterface, Name], *args, **kwargs) -> ConnectorInterface:
        if isinstance(stream_or_name, str):
            name = stream_or_name
            context = self.get_context()
            stream = context.get_stream(name) if context else None
        else:  # is_stream
            name = stream_or_name.get_name()
            stream = stream_or_name
        name = name.replace(':', '_')
        mask = self.get_children().get(name)
        if not mask:
            mask = TemporaryFilesMask(name, *args, stream=stream, parent=self, **kwargs)
        self.get_children()[name] = mask
        return mask

    def clear_all(self, forget: bool = True, verbose: bool = True) -> int:
        files = list(self.all_existing_files())
        self.log('Removing {} files from {}...'.format(len(files), self.get_path()), verbose=verbose)
        count = 0
        for f in files:
            count += f.remove(verbose=False)
            if forget:
                self.forget_child(f, also_from_context=True, skip_errors=True)
        self.log('Removed {} files from {}.'.format(count, self.get_path()), verbose=verbose)
        return count


class TemporaryFilesMask(ct.FileMask, TemporaryFilesMaskInterface):
    def __init__(
            self,
            name: Name,
            encoding: str = DEFAULT_ENCODING,
            stream: Optional[Stream] = None,
            parent: Parent = arg.DEFAULT,
            context: Context = arg.DEFAULT,
            verbose: Verbose = arg.DEFAULT,
    ):
        parent = arg.undefault(parent, TemporaryLocation(context=context))
        assert hasattr(parent, 'get_str_mask_template'), 'got {}'.format(parent)
        location_mask = parent.get_str_mask_template()
        assert arg.is_formatter(location_mask, 2)
        stream_mask = location_mask.format(name, PART_PLACEHOLDER)
        super().__init__(
            mask=stream_mask,
            parent=parent,
            context=context,
            verbose=verbose,
        )
        self.encoding = encoding
        self.stream = stream

    def get_encoding(self) -> str:
        return self.encoding

    def remove_all(self, log: bool = True, forget: bool = True) -> int:
        count = 0
        files = list(self.get_files())
        for file in files:
            assert isinstance(file, ct.AbstractFile)
            if file.is_existing():
                count += file.remove(log=log)
            if forget:
                self.forget_child(file, also_from_context=True)
        return count

    def get_files(self) -> Iterable:
        return self.get_children().values()

    def get_items(self, how: str = 'records', *args, **kwargs) -> Iterable:
        for file in self.get_files():
            yield from file.get_items(how=how, *args, **kwargs)

    def get_items_count(self) -> int:
        count = 0
        for file in self.get_files():
            count += file.get_count()
        return count

    def get_files_count(self) -> int:
        return len(self.get_children())

    def get_count(self, count_items: bool = True) -> int:
        if count_items:
            return self.get_items_count()
        else:
            return self.get_files_count()

    def get_sorted_items(
            self, key_function: Callable, reverse: bool = False,
            return_count=False, remove_after=False, verbose=True,
    ) -> Iterable:
        parts = self.get_children().values()
        assert parts, 'streams must be non-empty'
        iterables = [f.get_items() for f in parts]
        counts = [f.get_count() or 0 for f in parts]
        self.log('Merging {} parts...'.format(len(iterables)), verbose=verbose)
        merged_items = algo.merge_iter(iterables, key_function=key_function, reverse=reverse)
        if return_count:
            yield sum(counts)
            yield merged_items
        else:
            yield from merged_items
            if remove_after:
                self.remove_all()
