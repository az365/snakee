from abc import abstractmethod
from typing import Optional

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from interfaces import Context, ConnectorInterface, ConnType, Class, Name
    from connectors.abstract.abstract_storage import AbstractStorage
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import Context, ConnectorInterface, ConnType, Class, Name
    from ..abstract.abstract_storage import AbstractStorage

COVERT_PROPS = 'access_key', 'secret_key'
DEFAULT_PATH_DELIMITER = '/'
FIRST_PATH_DELIMITER = '://'
DEFAULT_S3_SERVICE_NAME = 's3'
DEFAULT_S3_ENDPOINT_URL = 'https://storage.yandexcloud.net'


class AbstractObjectStorage(AbstractStorage):
    def __init__(self, name: Name, context: Context, verbose=True):
        super().__init__(name=name, context=context, verbose=verbose)

    @abstractmethod
    def get_service_name(self) -> Name:
        pass

    def get_path_prefix(self) -> str:
        return self.get_service_name() + FIRST_PATH_DELIMITER

    def get_path_delimiter(self) -> str:
        return DEFAULT_PATH_DELIMITER


class S3Storage(AbstractObjectStorage):
    def __init__(
            self,
            name: Name = DEFAULT_S3_SERVICE_NAME,
            endpoint_url: str = DEFAULT_S3_ENDPOINT_URL,
            access_key: Optional[str] = None,
            secret_key: Optional[str] = None,
            context: Context = None,
            verbose: bool = True,
    ):
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        super().__init__(name=name, context=context, verbose=verbose)

    def get_service_name(self) -> Name:
        return DEFAULT_S3_SERVICE_NAME

    def get_endpoint_url(self) -> str:
        return self._endpoint_url

    def get_access_key(self) -> Optional[str]:
        return self._access_key

    def set_access_key(self, access_key: str):
        self._access_key = access_key
        return self

    def get_secret_key(self) -> Optional[str]:
        return self._secret_key

    def set_secret_key(self, secret_key: str):
        self._secret_key = secret_key
        return self

    @staticmethod
    def get_default_path_delimiter() -> str:
        return DEFAULT_PATH_DELIMITER

    @staticmethod
    def get_default_child_type() -> ConnType:
        return ConnType.S3Bucket

    def get_buckets(self) -> dict:
        return self.get_children()

    def bucket(
            self,
            name: Name,
            access_key: Optional[str] = None,
            secret_key: Optional[str] = None,
    ) -> ConnectorInterface:
        bucket = self.get_buckets().get(name)
        if bucket:
            if hasattr(bucket, 'set_access_key') and access_key is not None:
                bucket.set_access_key(access_key)
            if hasattr(bucket, 'set_secret_key') and secret_key is not None:
                bucket.set_secret_key(secret_key)
        else:
            bucket_class = self.get_default_child_obj_class()
            if access_key is None:
                access_key = self.get_access_key()
            if secret_key is None:
                secret_key = self.get_secret_key()
            bucket = bucket_class(name=name, storage=self, access_key=access_key, secret_key=secret_key)
        return bucket

    def get_resource_properties(self) -> dict:
        return dict(
            service_name=self.get_service_name(),
            endpoint_url=self.get_endpoint_url(),
        )

    @staticmethod
    def _get_covert_props() -> tuple:
        return COVERT_PROPS


ConnType.add_classes(S3Storage)
