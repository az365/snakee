from typing import Optional, Callable, Iterable, Sequence, Union
from inspect import isclass

try:  # Assume we're a submodule in a package.
    from content.items.simple_items import Class
    from base.functions.errors import get_type_err_msg, get_loc_message
    from base.interfaces.tree_interface import TreeInterface
    from base.interfaces.context_interface import ContextInterface
    from base.abstract.abstract_base import AbstractBaseObject
    from base.abstract.contextual_data import ContextualDataWrapper
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...content.items.simple_items import Class
    from ..functions.errors import get_type_err_msg, get_loc_message
    from ..interfaces.tree_interface import TreeInterface
    from ..interfaces.context_interface import ContextInterface
    from .abstract_base import AbstractBaseObject
    from .contextual_data import ContextualDataWrapper

Context = Optional[ContextInterface]
Parent = Union[Context, TreeInterface]
Child = Optional[ContextualDataWrapper]
NameOrChild = Union[str, ContextualDataWrapper]
OptionalFields = Optional[Union[str, Iterable]]

META_MEMBER_MAPPING = dict(_data='children', _source='parent')


class TreeItem(ContextualDataWrapper, TreeInterface):
    _parent_obj_classes: Sequence = list()
    _child_obj_classes: Sequence = list()

    def __init__(
            self,
            name: str,
            caption: str = '',
            parent: Parent = None,
            children: Optional[dict] = None,
            context: Context = None,
            check: bool = True,
    ):
        self._assert_is_appropriate_parent(parent, skip_missing=True)
        if not children:
            children = dict()
        super().__init__(name=name, caption=caption, data=children, source=parent, context=context, check=check)

    @classmethod
    def _is_tree_item(cls, item):
        return hasattr(item, 'get_name') and hasattr(item, 'get_parent') and hasattr(item, 'get_children')

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    def get_parent(self) -> Parent:
        return super().get_source()

    def set_parent(self, parent: Parent, reset: bool = False, inplace: bool = True) -> Optional[TreeInterface]:
        assert self._is_tree_item(parent), f'Expected TreeInterface, got {parent}'
        msg = 'Expected one of {} instance, got {} as {}'
        assert self._is_appropriate_parent(parent), msg.format(self.get_parent_obj_classes(), parent, type(parent))
        return self.set_source(parent, reset=reset, inplace=inplace)

    def get_children(self) -> dict:
        return super().get_data()

    def get_child(self, name: str) -> Child:
        children = self.get_children()
        assert isinstance(children, dict) or hasattr(children, 'get'), get_type_err_msg(children, dict, arg='children')
        return children.get(name)

    def _get_name_and_child(self, name_or_child: NameOrChild) -> tuple:
        if isinstance(name_or_child, str):
            name = name_or_child
            child = self.get_context().get_child(name)
        else:
            child = name_or_child
            if isinstance(child, ContextualDataWrapper) or hasattr(child, 'get_name'):
                name = child.get_name()
            else:
                msg = get_type_err_msg(expected=('DataWrapper', str), got=child, arg='name_or_child')
                raise TypeError(msg)
        return name, child

    def add_child(self, name_or_child: NameOrChild, check: bool = True, inplace: bool = True) -> Optional[Child]:
        children = self.get_children()
        name, child = self._get_name_and_child(name_or_child)
        if name in children:
            if check and self.get_child(name) != child:
                msg = f'child with name {name} already registered in {repr(self)}'
                raise ValueError(get_loc_message(msg=msg, caller=self.add_child))
        children[name] = child
        if hasattr(child, 'set_parent'):
            child.set_parent(self)
        if not inplace:
            return self

    def get_items(self) -> Iterable:
        yield from self.get_children().values()

    def close(self) -> int:
        total_closed_count = 0
        for child in self.get_children().values():
            if hasattr(child, 'close'):
                try:
                    current_closed_count = child.close(recursively=False)
                except TypeError:
                    current_closed_count = child.close()
                total_closed_count += current_closed_count or 0
        return total_closed_count

    def forget_child(
            self,
            name_or_child: NameOrChild,
            recursively: bool = False,
            also_from_context: bool = True,
            skip_errors: bool = False,
    ) -> int:
        name, child = self._get_name_and_child(name_or_child)
        if name in self.get_children() or skip_errors:
            child = self.get_children().pop(name)
            if child:
                child.close()
                count = 1
                if recursively and hasattr(child, 'get_children'):
                    for c in child.get_children():
                        count += c.forget_all_children()
                if also_from_context:
                    context = self.get_context()
                    if context:
                        context.forget_child(child, recursively=recursively, skip_errors=skip_errors)
            else:
                count = 0
            return count
        else:
            msg = f'child {child} with name {name} not registered'
            raise ValueError(get_loc_message(msg=msg, caller=self.forget_child))

    def forget_all_children(self) -> int:
        count = 0
        children = self.get_children().copy()
        for name in children:
            count += self.forget_child(name)
        return count

    def get_data(self) -> Iterable:
        for _, i in self.get_children().items():
            if hasattr(i, 'get_data'):
                yield from i.get_data()

    def close_child(self, name_or_child) -> int:
        child = self.get_child(name_or_child)
        return child.close()

    def has_children(self) -> bool:
        return bool(self.get_children())

    def is_leaf(self) -> bool:
        return not self.has_children()

    def is_root(self) -> bool:
        parent = self.get_parent()
        if parent:
            if not parent.is_context():
                return False
        return True

    @staticmethod
    def is_context() -> bool:
        return False

    def get_context(self) -> Parent:
        parent = self.get_parent()
        if parent is not None:
            if parent.is_context():
                return parent
            elif hasattr(parent, 'get_context'):
                return parent.get_context()

    def get_breadcrumbs(self):
        if self.is_root():
            hierarchy = list()
        else:
            hierarchy = self.get_parent().get_names_hierarchy()
        return hierarchy + [self.get_name()]

    def _get_meta_args(self) -> list:
        meta_args = super()._get_meta_args()
        meta_args.pop(1)
        return meta_args

    def __str__(self):
        cls_name = self.__class__.__name__
        meta = self.get_str_meta()
        str_repr = f'{cls_name}({meta})'
        obj = self
        while hasattr(obj, 'get_parent') and obj.get_parent():
            obj = obj.get_parent()
            str_repr = f'{repr(obj)}.{str_repr}'
        return f'<{str_repr}>'

    @classmethod
    def set_parent_obj_classes(cls, classes: Sequence) -> None:
        cls._parent_obj_classes = classes

    @classmethod
    def set_child_obj_classes(cls, classes: Sequence) -> None:
        cls._child_obj_classes = classes

    @classmethod
    def get_parent_obj_classes(cls) -> tuple:
        return tuple(cls._parent_obj_classes)

    @classmethod
    def get_child_obj_classes(cls) -> tuple:
        return tuple(cls._child_obj_classes)

    @classmethod
    def get_default_parent_obj_class(cls, skip_missing: bool = False) -> Optional[Class]:
        classes = cls.get_parent_obj_classes()
        if classes:
            return classes[0]
        elif not skip_missing:
            raise ValueError(get_loc_message(msg='parent classes for not set', caller=cls.get_default_parent_obj_class))

    @classmethod
    def get_default_child_obj_class(cls, skip_missing: bool = False) -> Optional[Class]:
        classes = cls.get_child_obj_classes()
        if classes:
            return classes[0]
        elif not skip_missing:
            raise ValueError(get_loc_message(msg='child classes for not set', caller=cls.get_default_child_obj_class))

    @staticmethod
    def _is_appropriate_class(
            obj,
            expected_classes: Union[list, tuple],
            skip_missing: bool = False,
            by_name: bool = True,
    ) -> bool:
        if obj is None and skip_missing:
            return True
        if expected_classes:
            if isinstance(obj, expected_classes):
                return True
            elif by_name:  # safe mode
                if isclass(obj):
                    obj_class = obj
                else:
                    obj_class = obj.__class__
                for c in expected_classes:
                    if obj_class.__name__ == c.__name__:
                        return True
            return False
        else:
            return True

    @classmethod
    def _is_appropriate_parent(cls, obj, skip_missing: bool = False, by_name: bool = True) -> bool:
        classes = cls.get_parent_obj_classes()
        return cls._is_appropriate_class(obj, classes, skip_missing=skip_missing, by_name=by_name)

    @classmethod
    def _is_appropriate_child(cls, obj, skip_missing: bool = False, by_name: bool = True) -> bool:
        classes = cls.get_child_obj_classes()
        return cls._is_appropriate_class(obj, classes, skip_missing=skip_missing, by_name=by_name)

    @classmethod
    def _assert_is_appropriate_parent(cls, obj, skip_missing: bool = False, caller: Callable = None) -> None:
        if not cls._is_appropriate_parent(obj, skip_missing=skip_missing):
            if caller is None:
                caller = cls._assert_is_appropriate_child
            msg = get_type_err_msg(expected=cls.get_parent_obj_classes(), got=obj, arg='parent', caller=caller)
            raise TypeError(msg)

    @classmethod
    def _assert_is_appropriate_child(
            cls,
            obj,
            skip_missing: bool = False,
            caller: Optional[Callable] = None,
    ) -> None:
        if not cls._is_appropriate_child(obj, skip_missing=skip_missing):
            if caller is None:
                caller = cls._assert_is_appropriate_child
            msg = get_type_err_msg(expected=cls.get_child_obj_classes(), got=obj, arg='child', caller=caller)
            raise TypeError(msg)
