import io
from typing import Optional

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import boto3
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from utils.external import boto3
    from .. import connector_classes as ct


class S3Bucket(ct.HierarchicFolder):
    def __init__(
            self,
            name,
            storage,
            verbose=True,
            access_key=arg.AUTO,
            secret_key=arg.AUTO,
    ):
        super().__init__(
            name=name,
            parent=storage,
            verbose=verbose,
        )
        self.access_key = arg.undefault(access_key, self.get_storage().access_key)
        self.secret_key = arg.undefault(secret_key, self.get_storage().secret_key)
        self.session = None
        self.client = None
        self.resource = None

    def get_default_child_class(self):
        return S3Folder

    def child(self, name: str, **kwargs):
        return super().child(name, parent_field='bucket', **kwargs)

    def folder(self, name: str, **kwargs):
        return self.child(name, **kwargs)

    def object(self, name: str, folder_name='', folder_kwargs=None, **kwargs):
        return self.folder(folder_name, **(folder_kwargs or {})).object(name, **kwargs)

    def get_bucket_name(self):
        return self.get_name()

    def get_session(self, props: Optional[dict] = None):
        if not self.session:
            self.reset_session(props)
        return self.session

    def reset_session(self, props: Optional[dict] = None):
        if not boto3:
            raise ImportError('boto3 must be installed (pip install boto3)')
        if not props:
            props = dict(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        self.session = boto3.session.Session(**props)

    def get_client(self, props=None):
        if not self.client:
            self.reset_client(props)
        return self.client

    def reset_client(self, props=None):
        if not props:
            props = self.get_storage().get_resource_properties()
        self.client = self.get_session().client(**props)

    def get_resource(self, props=None):
        if not self.resource:
            self.reset_resource(props)
        return self.resource

    def reset_resource(self, props=None):
        if not props:
            props = self.get_storage().get_resource_properties()
        self.resource = self.get_session().resource(**props)

    def list_objects(self, params: Optional[dict] = None, v2=False, field: Optional[str] = 'Contents'):
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

    def yield_objects(self, params: Optional[dict] = None):
        continuation_token = None
        if not params:
            params = dict()
        if 'MaxKeys' not in params:
            params['MaxKeys'] = 1000
        while True:
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            response = self.list_objects(params=params, v2=True, field=None)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

    def yield_object_names(self):
        for obj in self.yield_objects():
            yield obj['Key']

    def list_object_names(self):
        return list(self.yield_object_names())

    def list_prefixes(self):
        return self.list_objects(field='CommonPrefixes')

    def get_object(self, object_path_in_bucket):
        return self.get_resource().Object(self.get_bucket_name(), object_path_in_bucket)

    def get_buffer(self, object_path_in_bucket):
        buffer = io.BytesIO()
        self.get_object(object_path_in_bucket).download_fileobj(buffer)
        return buffer


class S3Folder(ct.FlatFolder):
    def __init__(
            self,
            name,
            bucket,
            verbose=arg.AUTO,
    ):
        super().__init__(
            name=name,
            parent=bucket,
            verbose=verbose,
        )

    def get_default_child_class(self):
        return ct.S3Object

    def get_bucket(self):
        return self.get_parent()

    def child(self, name: str, **kwargs):
        return super().child(name, parent_field='folder', **kwargs)

    def object(self, name: str, **kwargs):
        return self.child(name, **kwargs)

    def get_buffer(self, object_path_in_bucket):
        return self.get_bucket().get_buffer(object_path_in_bucket)
