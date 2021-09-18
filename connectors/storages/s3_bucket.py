import io
from typing import Optional, Iterable, Generator, Union, Type

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import boto3
    from interfaces import ConnectorInterface, Name
    from connectors.abstract.abstract_folder import HierarchicFolder
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import boto3
    from ...interfaces import ConnectorInterface, Name
    from ..abstract.abstract_folder import HierarchicFolder
    from .. import connector_classes as ct

DEFAULT_KEYS_LIMIT = 1000


class S3Bucket(HierarchicFolder):
    def __init__(
            self,
            name: Name,
            storage: ConnectorInterface,
            verbose: bool = True,
            access_key: str = arg.AUTO,
            secret_key: str = arg.AUTO,
    ):
        assert isinstance(storage, ct.S3Storage), 'S3Storage expected, got {}'.format(storage)
        self.access_key = arg.acquire(access_key, self.get_storage().access_key)
        self.secret_key = arg.acquire(secret_key, self.get_storage().secret_key)
        self.session = None
        self.client = None
        self.resource = None
        super().__init__(
            name=name,
            parent=storage,
            verbose=verbose,
        )

    def get_default_child_class(self) -> Type:
        return ct.S3Folder

    def child(self, name: Name, **kwargs) -> ConnectorInterface:
        return super().child(name, parent_field='bucket', **kwargs)

    def folder(self, name: Name, **kwargs) -> ConnectorInterface:
        return self.child(name, **kwargs)

    def object(self, name: Name, folder_name='', folder_kwargs=None, **kwargs) -> ConnectorInterface:
        folder = self.folder(folder_name, **(folder_kwargs or {}))
        assert isinstance(folder, ct.S3Folder)
        return folder.object(name, **kwargs)

    def get_bucket_name(self) -> Name:
        return self.get_name()

    def get_session(self, props: Optional[dict] = None):
        if not self.session:
            self.reset_session(props)
        return self.session

    def reset_session(self, props: Optional[dict] = None, inplace: bool = True):
        if not boto3:
            raise ImportError('boto3 must be installed (pip install boto3)')
        if not props:
            props = dict(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        session = boto3.session.Session(**props)
        self.session = session
        return session if inplace else self

    def get_client(self, props: Optional[dict] = None):
        if not self.client:
            self.reset_client(props)
        return self.client

    def reset_client(self, props: Optional[dict] = None, inplace: bool = True):
        if not props:
            props = self.get_storage().get_resource_properties()
        client = self.get_session().client(**props)
        self.client = client
        return client if inplace else self

    def get_resource(self, props: Optional[dict] = None):
        if not self.resource:
            self.reset_resource(props)
        return self.resource

    def reset_resource(self, props: Optional[dict] = None, inplace: bool = True):
        if not props:
            props = self.get_storage().get_resource_properties()
        resource = self.get_session().resource(**props)
        self.resource = resource
        return resource if inplace else self

    def list_objects(
            self,
            params: Optional[dict] = None,
            v2: bool = False,
            field: Optional[str] = 'Contents',
    ) -> Union[dict, list]:
        if not params:
            params = dict()
        if 'Bucket' not in params:
            params['Bucket'] = self.get_name()
        if 'Delimiter' not in params:
            params['Delimiter'] = self.get_path_delimiter()
        client = self.get_client(self.get_storage().get_resource_properties())
        if v2:
            objects = client.list_objects_v2(**params)
        else:
            objects = client.list_objects(**params)
        if field:
            return objects[field]
        else:
            return objects

    def yield_objects(self, params: Optional[dict] = None) -> Generator:
        continuation_token = None
        if not params:
            params = dict()
        if 'MaxKeys' not in params:
            params['MaxKeys'] = DEFAULT_KEYS_LIMIT
        while True:
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            response = self.list_objects(params=params, v2=True, field=None)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

    def yield_object_names(self) -> Generator:
        for obj in self.yield_objects():
            yield obj['Key']

    def list_object_names(self) -> list:
        return list(self.yield_object_names())

    def list_prefixes(self) -> Iterable:
        return self.list_objects(field='CommonPrefixes')

    def get_object(self, object_path_in_bucket: str):
        return self.get_resource().Object(self.get_bucket_name(), object_path_in_bucket)

    def get_buffer(self, object_path_in_bucket: str):
        buffer = io.BytesIO()
        self.get_object(object_path_in_bucket).download_fileobj(buffer)
        return buffer
