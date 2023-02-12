from typing import Optional, Generator

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from utils.external import boto3
    from interfaces import ConnectorInterface, ConnType, Class, Name
    from connectors.abstract.abstract_folder import HierarchicFolder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ...utils.external import boto3
    from ...interfaces import ConnectorInterface, ConnType, Class, Name
    from ..abstract.abstract_folder import HierarchicFolder

Response = dict


class S3Folder(HierarchicFolder):
    def __init__(self, name: Name, bucket: ConnectorInterface, verbose: Optional[bool] = None):
        bucket = self._assume_native(bucket)
        super().__init__(name=name, parent=bucket, verbose=verbose)

    def get_conn_type(self) -> ConnType:
        return ConnType.S3Folder

    @staticmethod
    def get_default_parent_type() -> ConnType:
        return ConnType.S3Bucket

    @staticmethod
    def get_default_child_type() -> ConnType:
        return ConnType.S3Object

    def get_bucket(self) -> ConnectorInterface:
        bucket = self.get_parent()
        return self._assume_native(bucket)

    def child(self, name: Name, **kwargs) -> ConnectorInterface:
        return super().child(name, parent_field='folder', **kwargs)

    def object(self, name: Name, **kwargs) -> ConnectorInterface:
        return self.child(name, **kwargs)

    def get_buffer(self, object_path_in_bucket: str):
        bucket = self.get_bucket()
        if hasattr(bucket, 'get_buffer'):
            return bucket.get_buffer(object_path_in_bucket)

    def is_directly_inside_bucket(self) -> bool:
        parent = self.get_parent()
        assert isinstance(parent, ConnectorInterface) or hasattr(parent, 'get_conn_type'), 'got {}'.format(parent)
        return parent.get_conn_type() == ConnType.S3Bucket

    def is_existing(self) -> bool:
        parent = self.get_parent()
        assert isinstance(parent, ConnectorInterface) or hasattr(parent, 'is_existing'), 'got {}'.format(parent)
        if parent.is_existing():
            if hasattr(parent, 'list_existing_folder_names'):  # isinstance(parent, (S3Bucket, S3Folder))
                return self.get_name() in parent.list_existing_folder_names()
            else:
                raise TypeError('Expected parent object as S3Bucket or S3Folder, got {}'.format(parent))
        else:
            return False

    def get_path_in_bucket(self) -> str:
        if self.is_directly_inside_bucket():
            return self.get_name()
        else:
            parent_folder = self.get_parent()
            if hasattr(parent_folder, 'get_path_in_bucket'):  # isinstance(parent_folder, S3Folder)
                parent_path = parent_folder.get_path_in_bucket()
            else:
                raise TypeError('Expected parent object as S3Bucket or S3Folder, got {}'.format(parent_folder))
            return self.get_path_delimiter().join([parent_path, self.get_name()])

    def get_existing_prefixes(self) -> list:
        bucket = self.get_bucket()
        if hasattr(bucket, 'get_existing_prefixes'):  # isinstance(bucket, S3Bucket)
            return bucket.get_existing_prefixes(prefix=self.get_path_in_bucket())
        else:
            raise TypeError('Expected parent bucket as S3Bucket, got {}'.format(bucket))

    def get_existing_object_props(self) -> Response:
        bucket = self.get_bucket()
        if hasattr(bucket, 'get_existing_object_props'):  # isinstance(bucket, S3Bucket)
            return bucket.get_existing_object_props(prefix=self.get_path_in_bucket())
        else:
            raise TypeError('Expected parent bucket as S3Bucket, got {}'.format(bucket))

    def get_existing_object_names(self) -> Generator:
        for object_props in self.get_existing_object_props():
            name = object_props.get('Key')
            if name:
                yield name

    def list_existing_names(self) -> list:
        return list(self.get_existing_object_names())

    def get_existing_folder_names(self) -> Generator:
        for prefix_props in self.get_existing_prefixes():
            name = prefix_props.get('Prefix')
            if name:
                yield name

    def list_existing_folder_names(self) -> list:
        return list(self.get_existing_folder_names())


ConnType.add_classes(S3Folder)
