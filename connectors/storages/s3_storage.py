from abc import abstractmethod

try:  # Assume we're a sub-module in a package.
    import context as fc
    from streams import stream_classes as sm
    from connectors import connector_classes as ct
    from utils import arguments as arg
    from loggers import logger_classes
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ... import context as fc
    from ...streams import stream_classes as sm
    from .. import connector_classes as ct
    from ...utils import arguments as arg
    from ...loggers import logger_classes


AUTO = arg.DEFAULT
CHUNK_SIZE = 8192
DEFAULT_PATH_DELIMITER = '/'
FIRST_PATH_DELIMITER = '://'
DEFAULT_S3_ENDPOINT_URL = 'https://storage.yandexcloud.net'


class AbstractObjectStorage(ct.AbstractStorage):
    def __init__(
            self,
            name,
            context,
            verbose=True,
    ):
        super().__init__(
            name=name,
            context=context,
            verbose=verbose,
        )

    @abstractmethod
    def get_default_child_class(self):
        pass

    @abstractmethod
    def get_service_name(self):
        pass

    def get_path_prefix(self):
        return self.get_service_name() + FIRST_PATH_DELIMITER

    def get_path_delimiter(self):
        return DEFAULT_PATH_DELIMITER


class S3Storage(AbstractObjectStorage):
    def __init__(
            self,
            name='s3',
            endpoint_url=DEFAULT_S3_ENDPOINT_URL,
            access_key=None,
            secret_key=None,
            context=None,
            verbose=True,
    ):
        super().__init__(
            name=name,
            context=context,
            verbose=verbose,
        )
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key

    @staticmethod
    def get_service_name():
        return 's3'

    def get_default_path_delimiter(self):
        return

    @staticmethod
    def get_default_child_class():
        return ct.S3Bucket

    def get_buckets(self):
        return self.get_children()

    def bucket(self, name, access_key=AUTO, secret_key=AUTO):
        bucket = self.get_buckets().get(name)
        if not bucket:
            bucket = ct.S3Bucket(
                name=name,
                storage=self,
                access_key=arg.undefault(access_key, self.access_key),
                secret_key=arg.undefault(secret_key, self.secret_key),
            )
        return bucket

    def get_resource_properties(self):
        return dict(
            service_name=self.get_service_name(),
            endpoint_url=self.endpoint_url,
        )
