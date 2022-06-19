from typing import Optional, Iterable, Generator, Union
import io

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoBool
    from utils.decorators import deprecated_with_alternative
    from utils.external import boto3, boto_core_client
    from interfaces import ConnType, ConnectorInterface, Class, Name
    from connectors.abstract.abstract_folder import HierarchicFolder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import AUTO, Auto, AutoBool
    from ...utils.decorators import deprecated_with_alternative
    from ...utils.external import boto3, boto_core_client
    from ...interfaces import ConnType, ConnectorInterface, Class, Name
    from ..abstract.abstract_folder import HierarchicFolder

Response = dict

DEFAULT_KEYS_LIMIT = 1000
COVERT_PROPS = 'access_key', 'secret_key'


class S3Bucket(HierarchicFolder):
    def __init__(
            self,
            name: Name,
            storage: ConnectorInterface,
            verbose: bool = True,
            access_key: str = AUTO,
            secret_key: str = AUTO,
    ):
        assert '_' not in name, 'Symbol "_" is not allowed for bucket name, use "-" instead'
        self._session = None
        self._client = None
        self._resource = None
        self._access_key = None
        self._secret_key = None
        storage = self._assume_native(storage)
        super().__init__(name=name, parent=storage, verbose=verbose)
        if Auto.is_defined(access_key):
            self.set_access_key(access_key)
        elif access_key == AUTO and hasattr(storage, 'get_access_key'):
            self.set_access_key(storage.get_access_key())
        if Auto.is_defined(secret_key):
            self.set_secret_key(secret_key)
        elif secret_key == AUTO and hasattr(storage, 'get_secret_key'):
            self.set_secret_key(storage.get_secret_key)

    def get_conn_type(self) -> ConnType:
        return ConnType.S3Bucket

    @staticmethod
    def get_default_child_type() -> ConnType:
        return ConnType.S3Folder

    def child(self, name: Name, **kwargs) -> ConnectorInterface:
        return super().child(name, parent_field='bucket', **kwargs)

    def folder(self, name: Name, **kwargs) -> ConnectorInterface:
        return self.child(name, **kwargs)

    def object(self, name: Name, folder_name='', folder_kwargs=None, **kwargs) -> ConnectorInterface:
        folder = self.folder(folder_name, **(folder_kwargs or {}))
        self._assert_is_appropriate_child(folder)
        if self._is_appropriate_child(folder) or hasattr(folder, 'object'):
            return folder.object(name, **kwargs)

    def is_accessible(self, verbose: bool = False) -> bool:
        try:
            self.list_objects(verbose=verbose)
            return True
        except:  # ConnectionError
            return False

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

    def get_bucket_name(self) -> Name:
        return self.get_name()

    def get_session(self, props: Optional[dict] = None):
        if not self._session:
            self.reset_session(props)
        return self._session

    def reset_session(self, props: Optional[dict] = None, inplace: bool = True):
        if not boto3:
            raise ImportError('boto3 must be installed (pip install boto3)')
        if not props:
            props = dict(
                aws_access_key_id=self.get_access_key(),
                aws_secret_access_key=self.get_secret_key(),
            )
        session = boto3.session.Session(**props)
        self._session = session
        return session if inplace else self

    def get_client(self, props: Optional[dict] = None):
        if not self._client:
            self.reset_client(props)
        return self._client

    def reset_client(self, props: Optional[dict] = None, inplace: bool = True):
        if not props:
            props = self.get_storage().get_resource_properties()
        client = self.get_session().client(**props)
        self._client = client
        return client if inplace else self

    def get_resource(self, props: Optional[dict] = None):
        if not self._resource:
            self.reset_resource(props)
        return self._resource

    def reset_resource(self, props: Optional[dict] = None, inplace: bool = True):
        if not props:
            props = self.get_storage().get_resource_properties()
        resource = self.get_session().resource(**props)
        self._resource = resource
        return resource if inplace else self

    def list_objects(
            self,
            params: Optional[dict] = None,
            v2: bool = False,
            field: Optional[str] = 'Contents',
            verbose: AutoBool = AUTO,
    ) -> Union[dict, list]:
        if not params:
            params = dict()
        if 'Bucket' not in params:
            params['Bucket'] = self.get_name()
        if 'Delimiter' not in params:
            params['Delimiter'] = self.get_path_delimiter()
        client = self.get_client(self.get_storage().get_resource_properties())
        if verbose:
            self.log(f'Getting objects from s3-bucket {self.get_name()}...', verbose=verbose)  # level=LoggingLevel.Debug
        if v2:
            objects = client.list_objects_v2(**params)
        else:
            objects = client.list_objects(**params)
        if field:
            return objects[field]
        else:
            return objects

    def yield_objects(self, params: Optional[dict] = None, verbose: AutoBool = AUTO) -> Generator:
        continuation_token = None
        if not params:
            params = dict()
        if 'MaxKeys' not in params:
            params['MaxKeys'] = DEFAULT_KEYS_LIMIT
        while True:
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            response = self.list_objects(params=params, v2=True, field=None, verbose=verbose)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

    def yield_object_names(self, verbose: AutoBool = AUTO) -> Generator:
        for obj in self.yield_objects(verbose=verbose):
            yield obj['Key']

    def list_object_names(self, verbose: AutoBool = AUTO) -> list:
        return list(self.yield_object_names(verbose=verbose))

    def list_prefixes(self, verbose: AutoBool = AUTO) -> Iterable:
        return self.list_objects(field='CommonPrefixes', verbose=verbose)

    def get_object(self, object_path_in_bucket: str):
        return self.get_resource().Object(self.get_bucket_name(), object_path_in_bucket)

    def get_buffer(self, object_path_in_bucket: str):
        buffer = io.BytesIO()
        self.get_object(object_path_in_bucket).download_fileobj(buffer)
        return buffer

    def create(self, if_not_yet: bool = False, inplace: bool = False) -> Union[ConnectorInterface, Response]:
        if self.is_existing() and not if_not_yet:
            raise ValueError('Bucket {} already existing'.format(self))
        response = self.get_client().create_bucket(Bucket=self.get_bucket_name())
        if inplace:
            return response
        else:
            return self

    def is_existing(self) -> bool:
        resource = self.get_resource()
        try:
            resource.meta.client.head_bucket(Bucket=self.get_bucket_name())
            return True
        except boto_core_client.ClientError:
            return False

    def get_existing_prefixes(self, prefix: Optional[str] = None) -> list:
        return self.get_existing_object_props(prefix=prefix).get('CommonPrefixes', [])

    def get_existing_object_props(self, prefix: Optional[str] = None, **kwargs) -> Response:
        current_bucket_name = self.get_bucket_name()
        if 'Bucket' in kwargs:
            kwargs_bucket_name = kwargs['Bucket']
            assert kwargs_bucket_name == current_bucket_name, '{} != {}'.format(kwargs_bucket_name, current_bucket_name)
        else:
            kwargs['Bucket'] = current_bucket_name
        if prefix:
            if 'Prefix' in kwargs:
                kwargs_prefix = kwargs['Prefix']
                assert prefix == kwargs_prefix, '{} != {}'.format(prefix, kwargs_prefix)
            kwargs['Prefix'] = prefix
        return self.get_client().list_objects(**kwargs)

    def get_existing_object_names(self, prefix: Optional[str] = None) -> Generator:
        for object_props in self.get_existing_object_props(prefix=prefix).get('Contents', []):
            name = object_props.get('Key')
            if name:
                yield name

    def list_existing_names(self, prefix: Optional[str] = None) -> list:
        return list(self.get_existing_object_names(prefix=prefix))

    def get_existing_folder_names(self, prefix: Optional[str] = None) -> Generator:
        for prefix_props in self.get_existing_prefixes(prefix=prefix):
            name = prefix_props.get('Prefix')
            if name:
                yield name

    def list_existing_folder_names(self, prefix: Optional[str] = None) -> list:
        return list(self.get_existing_folder_names(prefix=prefix))

    @staticmethod
    def _get_covert_props() -> tuple:
        return COVERT_PROPS


ConnType.add_classes(S3Bucket)
