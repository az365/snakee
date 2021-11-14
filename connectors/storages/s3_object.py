from typing import Optional, Generator, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        ConnectorInterface, ContentFormatInterface, Stream, StructInterface,
        ContentType, StreamType,
        AUTO, Auto, AutoContext, AutoBool, AutoCount, Name,
    )
    from connectors.abstract.leaf_connector import LeafConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        ConnectorInterface, ContentFormatInterface, Stream, StructInterface,
        ContentType, StreamType,
        AUTO, Auto, AutoContext, AutoBool, AutoCount, Name,
    )
    from ..abstract.leaf_connector import LeafConnector

DEFAULT_STORAGE_CLASS = 'COLD'


class S3Object(LeafConnector):
    def __init__(
            self,
            name: Name,
            content_format: Union[ContentFormatInterface, Auto] = AUTO,
            struct: Union[StructInterface, Auto, None] = AUTO,
            folder: ConnectorInterface = None,
            context: AutoContext = AUTO,
            expected_count: AutoCount = AUTO,
            verbose: AutoBool = AUTO,
    ):
        super().__init__(
            name=name,
            content_format=content_format, struct=struct,
            expected_count=expected_count,
            parent=folder, context=context, verbose=verbose,
        )

    def get_content_type(self) -> ContentType:
        return ContentType.TextFile

    def get_folder(self) -> ConnectorInterface:
        parent = self.get_parent()
        return self._assume_connector(parent)

    def get_bucket(self) -> ConnectorInterface:
        return self.get_folder().get_bucket()

    def get_bucket_name(self) -> Name:
        return self.get_bucket().get_name()

    def get_object_path_in_bucket(self) -> str:
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

    def get_first_line(self, close: bool = True) -> Optional[str]:
        iter_lines = self.get_lines()
        try:
            first_line = next(iter_lines)
        except StopIteration:
            first_line = None
        if close:
            self.close()
        return first_line

    def get_lines(self) -> Generator:
        for line in self.get_body():
            yield line.decode('utf8', errors='ignore')

    def get_data(self) -> Generator:
        return self.get_lines()

    def put_object(self, data, storage_class=DEFAULT_STORAGE_CLASS):
        return self.get_client().put_object(
            Bucket=self.get_bucket_name(),
            Key=self.get_object_path_in_bucket(),
            Body=data,
            StorageClass=storage_class,
        )

    def upload_file(self, file: Union[LeafConnector, str], extra_args={}):
        if isinstance(file, str):
            filename = file
        elif isinstance(file, LeafConnector):
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

    def is_existing(self) -> bool:
        return self.get_object_path_in_bucket() in self.get_bucket().list_object_names()

    def from_stream(self, stream: Stream, storage_class=DEFAULT_STORAGE_CLASS, verbose: bool = True):
        return self.put_object(data=stream.get_items(), storage_class=storage_class)

    def to_stream(self, stream_type: Union[StreamType, str, Auto] = AUTO, **kwargs) -> Stream:
        stream_class = StreamType(stream_type).get_class()
        return stream_class(self.get_data(), **kwargs)
