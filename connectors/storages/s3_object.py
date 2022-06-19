from typing import Optional, Generator, Iterator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnectorInterface, ContentFormatInterface, Stream, StructInterface,
        ContentType, ConnType, StreamType,
        AUTO, Auto, AutoContext, AutoBool, AutoCount, AutoName, Name,
    )
    from connectors.abstract.leaf_connector import LeafConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ConnectorInterface, ContentFormatInterface, Stream, StructInterface,
        ContentType, ConnType, StreamType,
        AUTO, Auto, AutoContext, AutoBool, AutoCount, AutoName, Name,
    )
    from ..abstract.leaf_connector import LeafConnector

Response = dict

DEFAULT_STORAGE_CLASS = 'COLD'
HTTP_OK = 200


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

    def is_accessible(self, verbose: bool = False) -> bool:
        return self.get_bucket().is_accessible(verbose=verbose)

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

    def get_object_response(self) -> Response:
        return self.get_bucket().get_client().get_object(
            Bucket=self.get_bucket().get_name(),
            Key=self.get_object_path_in_bucket(),
        )

    def get_body(self):
        return self.get_object_response()['Body']

    def get_next_lines(self, count: AutoCount = None) -> Iterator[str]:
        prev_line = ''
        for b, buffer in enumerate(self.get_body()):
            lines = buffer.decode('utf8', errors='ignore').split('\n')
            cnt = len(lines)
            for n, line in enumerate(lines):
                if n == 0:
                    line = prev_line + line
                is_last = n >= cnt - 1
                if is_last:
                    prev_line = line
                else:
                    yield line
            if Auto.is_defined(count):
                if b >= count:
                    break
        if prev_line:
            yield prev_line

    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False,
            skip_missing: bool = False,
            allow_reopen: bool = True,
            verbose: AutoBool = AUTO,
            message: AutoName = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterator[str]:
        if skip_missing:
            if not self.is_existing():
                return None
        lines = self.get_next_lines()
        verbose = Auto.acquire(verbose, self.is_verbose())
        if verbose or Auto.is_defined(message):
            if not Auto.is_defined(message):
                message = 'Reading {}'
            if '{}' in message:
                message = message.format(self.get_name())
            logger = self.get_logger()
            assert hasattr(logger, 'progress'), '{} has no progress in {}'.format(self, logger)
            if not count:
                count = self.get_count(allow_slow_mode=False)
            lines = self.get_logger().progress(lines, name=message, count=count, step=step)
        for n, i in enumerate(lines):
            if Auto.is_defined(count):
                if n >= count:
                    break
            if skip_first and n == 0:
                pass
            else:
                yield i

    def get_data(self) -> Iterator[str]:
        return self.get_lines()

    def put_object(self, data, storage_class=DEFAULT_STORAGE_CLASS) -> Response:
        return self.get_client().put_object(
            Bucket=self.get_bucket_name(),
            Key=self.get_object_path_in_bucket(),
            Body=data,
            StorageClass=storage_class,
        )

    def upload_file(self, file: Union[LeafConnector, str], extra_args={}) -> Response:
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

    def is_existing(self, verbose: AutoBool = AUTO) -> bool:
        bucket = self.get_bucket()
        if bucket.is_existing(verbose=verbose):
            if hasattr(bucket, 'list_object_names'):  # isinstance(bucket, S3Bucket)
                return self.get_object_path_in_bucket() in bucket.list_object_names(verbose=verbose)
            else:
                raise TypeError('Expected bucket as S3Bucket, got {}'.format(bucket))
        else:
            return False

    def _get_lines_from_stream(self, stream: Stream) -> Generator:
        content_format = self.get_content_format()
        for i in stream.get_items():
            if hasattr(content_format, 'get_formatted_item'):
                line = content_format.get_formatted_item(i)
            else:
                line = str(i)
            yield line

    def from_stream(self, stream: Stream, storage_class=DEFAULT_STORAGE_CLASS, encoding='utf8', verbose: bool = True):
        lines = self._get_lines_from_stream(stream)
        data = bytes('\n'.join(lines), encoding=encoding)
        response = self.put_object(data=data, storage_class=storage_class)
        is_done = response.get('ResponseMetadata').get('HTTPStatusCode') == HTTP_OK
        if is_done:
            return self
        else:
            raise ValueError(response)

    def to_stream(self, stream_type: Union[StreamType, str, Auto] = AUTO, **kwargs) -> Stream:
        stream_class = StreamType(stream_type).get_class()
        return stream_class(self.get_data(), **kwargs)

    def get_expected_count(self) -> Optional[int]:
        return self._count

    def get_count(self, *args, **kwargs) -> Optional[int]:
        return None  # not available property

    def is_empty(self, verbose: bool = AUTO) -> Optional[bool]:
        verbose = Auto.delayed_acquire(verbose, self.is_verbose)
        if self.is_accessible():
            return not self.get_first_line(close=True, skip_missing=True, verbose=verbose)

    def get_modification_timestamp(self):
        path = self.get_object_path_in_bucket()
        bucket = self.get_bucket()
        for i in bucket.yield_objects():
            if i['Key'] == path:
                modification_datetime = i['LastModified']
                return modification_datetime.timestamp()


ConnType.add_classes(S3Object)
