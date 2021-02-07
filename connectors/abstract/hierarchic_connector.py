from abc import abstractmethod

try:  # Assume we're a sub-module in a package.
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import connector_classes as ct


class HierarchicConnector(ct.AbstractConnector):
    def __init__(
            self,
            name,
            parent=None,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        self.children = dict()

    def has_hierarchy(self):
        return True

    @abstractmethod
    def get_default_child_class(self):
        pass

    def get_child_class_by_name(self, name):
        return self.get_default_child_class()

    def child(self, name, **kwargs):
        cur_child = self.children.get(name)
        if not cur_child:
            child_class = self.get_child_class_by_name(name)
            cur_child = child_class(name, **kwargs)
            self.children[name] = cur_child
        return cur_child

    def get_children(self):
        return self.children

    def get_items(self):
        for name, child in self.get_children().items():
            yield child

    def get_meta(self):
        meta = super().get_meta()
        meta.pop('children')
        return meta
