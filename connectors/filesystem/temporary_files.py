try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
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
        self.mask = mask

    def get_str_mask_template(self):
        return self.mask

    def stream_mask(self, stream_or_name, *args, **kwargs):
        if isinstance(stream_or_name, str):
            name = stream_or_name
            context = self.get_context()
            stream = context.get_stream(name) if context else None
        else:  # is_stream
            name = stream_or_name.get_name()
            stream = stream_or_name
        mask = self.get_children().get(name)
        if not mask:
            mask = TemporaryFilesMask(name, *args, stream=stream, **kwargs)
        self.get_children()[name] = mask
        return TemporaryFilesMask(name, *args, )


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
        assert isinstance(parent, TemporaryLocation)
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

    def erase_all(self, log=True):
        for file in self.get_files():
            assert isinstance(file, ct.AbstractFile)
            if file.is_existing():
                file.remove(log=log)

    def get_items(self, how='records', *args, **kwargs):
        for file in self.get_files():
            yield from file.get_items(how=how, *args, **kwargs)
