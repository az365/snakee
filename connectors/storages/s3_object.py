try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from connectors import connector_classes as ct
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams import stream_classes as sm
    from .. import connector_classes as ct
    from ...utils import arguments as arg


class S3Object(ct.LeafConnector):
    def __init__(
            self,
            name,
            folder,
            verbose=arg.DEFAULT,
    ):
        assert isinstance(folder, ct.S3Folder)
        super().__init__(
            name=name,
            parent=folder,
        )
        self.verbose = arg.undefault(verbose, folder.verbose)

    def get_folder(self):
        return self.get_parent()

    def get_bucket(self):
        return self.get_folder().get_bucket()

    def get_bucket_name(self):
        return self.get_bucket().get_name()

    def get_object_path_in_bucket(self):
        if self.get_folder().get_name():
            return self.get_folder().get_name() + self.get_path_delimiter() + self.get_name()
        else:
            return self.get_name()

    def get_client(self):
        return self.get_bucket().get_client()

    def get_buffer(self):
        return self.get_folder().get_buffer(self.get_object_path_in_bucket())

    def get_object_response(self):
        return self.get_bucket().get_client().get_object(
            Bucket=self.get_bucket().get_name(),
            Key=self.get_object_path_in_bucket(),
        )

    def get_body(self):
        return self.get_object_response()['Body']

    def get_data(self):
        for line in self.get_body():
            yield line.decode('utf8', errors='ignore')

    def put_object(self, data, storage_class='COLD'):
        return self.get_client().put_object(
            Bucket=self.get_bucket_name(),
            Key=self.get_object_path_in_bucket(),
            Body=data,
            StorageClass=storage_class,
        )

    def upload_file(self, file, extra_args={}):
        if isinstance(file, str):
            filename = file
        elif isinstance(file, ct.LeafConnector):
            filename = file.get_path()
        else:
            message = 'file-argument must be path to local file or File(LeafConnector) object (got {} as {})'
            raise TypeError(message.format(file, type(file)))
        return self.get_client().upload_file(
            Filename=filename,
            Bucket=self.get_bucket_name(),
            Key=self.get_path(),
            ExtraArgs=extra_args,
        )

    def is_existing(self):
        return self.get_object_path_in_bucket() in self.get_bucket().list_object_names()

    def from_stream(self, stream, storage_class='COLD'):
        assert sm.is_stream(stream)
        return self.put_object(data=stream.iterable(), storage_class=storage_class)

    def to_stream(self, stream_type=arg.DEFAULT, **kwargs):
        stream_class = sm.get_class(stream_type)
        return stream_class(
            self.get_data(),
            **kwargs
        )
