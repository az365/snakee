try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from utils.external import boto3
    from interfaces import ConnectorInterface, ConnType, Class, Name, AutoBool, Auto, AUTO
    from connectors.abstract.abstract_folder import HierarchicFolder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated_with_alternative
    from ...utils.external import boto3
    from ...interfaces import ConnectorInterface, ConnType, Class, Name, AutoBool, Auto, AUTO
    from ..abstract.abstract_folder import HierarchicFolder


class S3Folder(HierarchicFolder):
    def __init__(self, name: Name, bucket: ConnectorInterface, verbose: AutoBool = AUTO):
        bucket = self._assume_native(bucket)
        super().__init__(name=name, parent=bucket, verbose=verbose)

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


ConnType.add_classes(S3Folder)
