from typing import Iterable, Union, Optional

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        Connector, Stream,
        FolderType, FileType, StreamType,
        AUTO, Auto, AutoName, AutoBool, AutoContext, OptionalFields,
    )
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.abstract.leaf_connector import LeafConnector
    from connectors.abstract.abstract_folder import HierarchicFolder
    from connectors.filesystem.local_folder import LocalFolder
    from connectors.filesystem.local_file import AbstractFile
    from connectors.filesystem.local_mask import LocalMask
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        Connector, Stream,
        FolderType, FileType, StreamType,
        AUTO, Auto, AutoName, AutoBool, AutoContext, OptionalFields,
    )
    from ..abstract.hierarchic_connector import HierarchicConnector
    from ..abstract.leaf_connector import LeafConnector
    from ..abstract.abstract_folder import HierarchicFolder
    from .local_folder import LocalFolder
    from .local_file import AbstractFile
    from .local_mask import LocalMask

Native = Union[LocalMask, AbstractFile]
Suffix = Union[str, int, None]


class PartitionedLocalFile(LocalMask, AbstractFile):
    def __init__(
            self,
            mask: str,
            suffix: Suffix,
            parent: HierarchicConnector,
            context: AutoContext = None,
            verbose: AutoBool = AUTO,
    ):
        self._suffix = None
        self._partition = None
        super().__init__(mask, parent, context=context, verbose=verbose)
        self.set_suffix(suffix, inplace=True)

    def get_suffix(self) -> Suffix:
        return self._suffix

    def set_suffix(self, suffix: Suffix, inplace: bool = True) -> Native:
        if inplace:
            self._suffix = suffix
            partition = self.file(suffix)
            assert isinstance(partition, AbstractFile)
            self.set_partition(partition, inplace=True)
            return self
        else:
            return self.make_new(suffix=suffix)

    def get_partition(self) -> AbstractFile:
        return self._partition

    def set_partition(self, partition: LeafConnector, inplace: bool) -> Native:
        if inplace:
            self._partition = partition
            return self
        else:
            suffix = self._extract_suffix_from_name(partition.get_name())
            return self.make_new(suffix=suffix)

    def _extract_suffix_from_name(self, name: str) -> str:
        mask_str = self.get_mask()
        prefix = mask_str.split('{')[0]
        postfix = mask_str.split('}')[1]
        prefix_len, postfix_len = len(prefix), len(postfix)
        suffix = name[prefix_len: -postfix_len]
        return suffix

    def file(self, suffix: Union[Suffix, arg.Auto], filetype: Union[FileType, Auto] = AUTO, **kwargs) -> Connector:
        acquired_suffix = arg.acquire(suffix, self.get_suffix())
        assert acquired_suffix, 'suffix must be defined, got argument {}, default {}'.format(suffix, self.get_suffix())
        filename = self.get_mask().format(acquired_suffix)
        return super().file(filename, filetype=filetype, **kwargs)

    def get_files(self) -> Iterable:
        return self.get_children().values()

    def get_items(self, how: str = 'records', *args, **kwargs) -> Iterable:
        for file in self.get_files():
            yield from file.get_items(how=how, *args, **kwargs)

    def get_items_count(self, allow_reopen=True, allow_slow_gzip=True, force=False) -> Optional[int]:
        partition = self.get_partition()
        if partition:
            if isinstance(partition, AbstractFile) or hasattr(partition, 'get_count'):
                return partition.get_count(allow_reopen=allow_reopen, allow_slow_gzip=allow_slow_gzip, force=force)
        else:
            count = 0
            for file in self.get_files():
                current_count = file.get_count(allow_reopen=allow_reopen, allow_slow_gzip=allow_slow_gzip, force=force)
                count += current_count or 0
            return count

    def get_files_count(self) -> int:
        return len(self.get_children())

    def get_count(self, count_items: bool = True, **kwargs) -> Optional[int]:
        if count_items:
            return self.get_items_count(**kwargs)
        else:
            assert not kwargs
            return self.get_files_count()

    def from_stream(self, stream: Stream, verbose: bool = True) -> Native:
        partition = self.get_partition()
        assert partition, 'suffix and partition must be defined'
        partition = partition.from_stream(stream, verbose=verbose)
        self.set_partition(partition, inplace=True)
        return self

    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO, name: AutoName = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO, ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        partition = self.get_partition()
        assert partition, 'suffix and partition must be defined'
        return partition.to_stream(data=data, name=name, stream_type=stream_type, ex=ex, **kwargs)


FolderType.set_dict_classes(
    {
        FolderType.LocalFolder: LocalFolder,
        FolderType.LocalMask: LocalMask,
        FolderType.PartitionedLocalFile: PartitionedLocalFile,
    }
)
