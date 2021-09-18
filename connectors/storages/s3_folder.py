try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import boto3
    from interfaces import ConnectorInterface, Name, AutoBool, Auto, AUTO
    from connectors.abstract.abstract_folder import FlatFolder
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import boto3
    from ...interfaces import ConnectorInterface, Name, AutoBool, Auto, AUTO
    from ..abstract.abstract_folder import FlatFolder
    from .. import connector_classes as ct


class S3Folder(FlatFolder):
    def __init__(self, name: Name, bucket: ConnectorInterface, verbose: AutoBool = AUTO):
        assert isinstance(bucket, ct.S3Bucket), 'S3Bucket expected, got {}'.format(bucket)
        super().__init__(name=name, parent=bucket, verbose=verbose)

    def get_default_child_class(self):
        return ct.S3Object

    def get_bucket(self) -> ConnectorInterface:
        bucket = self.get_parent()
        assert isinstance(bucket, ct.S3Bucket), 'S3Bucket expected, got {}'.format(bucket)
        return bucket

    def child(self, name: Name, **kwargs) -> ConnectorInterface:
        return super().child(name, parent_field='folder', **kwargs)

    def object(self, name: Name, **kwargs) -> ConnectorInterface:
        return self.child(name, **kwargs)

    def get_buffer(self, object_path_in_bucket: str):
        bucket = self.get_bucket()
        if hasattr(bucket, 'get_buffer'):
            return bucket.get_buffer(object_path_in_bucket)
