from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ContextInterface, StreamInterface, ConnectorInterface, LeafConnectorInterface,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        Context, Stream, Connector, TmpFiles,
        Name, Source,
    )
    from base.constants.chars import OS_PLACEHOLDER, PY_PLACEHOLDER
    from base.functions.errors import get_type_err_msg
    from utils.algo import merge_iter
    from functions.primary.text import is_formatter
    from connectors.filesystem.local_mask import LocalFolder, LocalMask
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ContextInterface, StreamInterface, ConnectorInterface, LeafConnectorInterface,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        Context, Stream, Connector, TmpFiles,
        Name, Source,
    )
    from ...base.constants.chars import OS_PLACEHOLDER, PY_PLACEHOLDER
    from ...base.functions.errors import get_type_err_msg
    from ...utils.algo import merge_iter
    from ...functions.primary.text import is_formatter
    from .local_mask import LocalFolder, LocalMask

DEFAULT_FOLDER = 'tmp'
DEFAULT_MASK = 'stream_{}_part{}.tmp'
PART_PLACEHOLDER = '{:03}'
DEFAULT_ENCODING = 'utf8'


class TemporaryLocation(LocalFolder, TemporaryLocationInterface):
    def __init__(
            self,
            path: Name = DEFAULT_FOLDER,
            mask: Name = DEFAULT_MASK,
            path_is_relative: bool = True,
            parent: Source = None,
            context: Context = None,
            verbose: Optional[bool] = None,
    ):
        mask = mask.replace(OS_PLACEHOLDER, PY_PLACEHOLDER)  # '*', '{}'
        assert is_formatter(mask, 2)
        self._mask = mask
        super().__init__(
            path=path, path_is_relative=path_is_relative,
            parent=parent, context=context,
            verbose=verbose,
        )

    def get_str_mask_template(self) -> Name:
        return self._mask

    def mask(self, mask: Name) -> ConnectorInterface:
        return self.stream_mask(mask)

    def stream_mask(self, stream_or_name: Union[StreamInterface, Name], *args, **kwargs) -> ConnectorInterface:
        if isinstance(stream_or_name, (str, int)):
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
        path = self.get_path()
        files = list(self.all_existing_files())
        found = len(files)
        self.log(f'Removing {found} files from {path}...', verbose=verbose)
        count = 0
        for f in files:
            count += f.remove(verbose=False)
            if forget:
                self.forget_child(f, also_from_context=True, skip_errors=True)
        self.log(f'Removed {count} files from {path}.', verbose=verbose)
        return count


class TemporaryFilesMask(LocalMask, TemporaryFilesMaskInterface):
    def __init__(
            self,
            name: Name,
            encoding: str = DEFAULT_ENCODING,
            stream: Optional[Stream] = None,
            parent: Source = None,
            context: Context = None,
            verbose: Optional[bool] = None,
    ):
        if parent is None:
            parent = TemporaryLocation(context=context)
        if isinstance(parent, TemporaryLocation) or hasattr(parent, 'get_str_mask_template'):
            location_mask = parent.get_str_mask_template()
        else:
            msg = get_type_err_msg(expected=TemporaryLocation, got=parent, arg='parent', caller=TemporaryFilesMask)
            raise TypeError(msg)
        if is_formatter(location_mask, count=2):
            stream_mask = location_mask.format(name, PART_PLACEHOLDER)
        else:
            msg = f'Expected location_mask with 2 placeholders (name, part), got {location_mask}'
            raise ValueError(msg)
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

    def remove_all(self, forget: bool = True, log: bool = True, verbose: bool = False) -> int:
        count = 0
        files = list(self.get_files())
        for file in files:
            if isinstance(file, LeafConnectorInterface) or hasattr(file, 'remove'):
                if file.is_existing():
                    count += file.remove(log=log, verbose=verbose)
                if forget:
                    self.forget_child(file, also_from_context=True)
            else:
                msg = get_type_err_msg(expected=LeafConnectorInterface, got=file, arg='file', caller=self.remove_all)
                raise TypeError(msg)
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
        parts_count = len(iterables)
        item_counts = [f.get_count() or 0 for f in parts]
        self.log(f'Merging {parts_count} parts...', verbose=verbose)
        merged_items = merge_iter(iterables, key_function=key_function, reverse=reverse)
        if return_count:
            yield sum(item_counts)
            yield merged_items
        else:
            yield from merged_items
            if remove_after:
                self.remove_all()
