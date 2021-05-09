from typing import Iterable, Callable

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        algo,
    )
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        algo,
    )
    from .. import connector_classes as ct

DEFAULT_FOLDER = 'tmp'
DEFAULT_MASK = 'stream_{}_part{}.tmp'
PART_PLACEHOLDER = '{:03}'
DEFAULT_ENCODING = 'utf8'


class TemporaryLocation(ct.LocalFolder):
    def __init__(
            self,
            path=DEFAULT_FOLDER,
            mask=DEFAULT_MASK,
            path_is_relative=True,
            parent=arg.DEFAULT,
            context=arg.DEFAULT,
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

    def get_str_mask_template(self):
        return self._mask

    def mask(self, mask: str):
        return self.stream_mask(mask)

    def stream_mask(self, stream_or_name, *args, **kwargs):
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


class TemporaryFilesMask(ct.FileMask):
    def __init__(
            self,
            name,
            encoding=DEFAULT_ENCODING,
            stream=None,
            parent=arg.DEFAULT,
            context=arg.DEFAULT,
            verbose=arg.DEFAULT,
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

    def get_encoding(self):
        return self.encoding

    def remove_all(self, log: bool = True) -> int:
        count = 0
        for file in self.get_files():
            assert isinstance(file, ct.AbstractFile)
            if file.is_existing():
                count += file.remove(log=log)
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
