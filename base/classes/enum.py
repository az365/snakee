from inspect import isclass
from functools import total_ordering
from typing import Optional, Callable, Iterable, Union, Type

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg

Name = str
Value = Union[str, int, arg.Auto]
Class = Union[Type, Callable]

AUX_NAMES = ('name', 'value', 'is_prepared')


@total_ordering
class EnumItem:
    _auto_value = True

    def __init__(self, name: Name, value: Union[Value, arg.Auto] = arg.AUTO, update: bool = False):
        if update or not self._is_initialized():
            name = arg.get_name(name)
            if self._auto_value:
                value = arg.acquire(value, name)
            self.name = name
            self.value = value

    def _is_initialized(self) -> bool:
        if hasattr(self, 'name') and hasattr(self, 'value'):
            if self.name or self.value:
                return True
        return False

    def get_name(self) -> Name:
        return self.name

    def get_value(self) -> Value:
        return self.value

    @staticmethod
    def _get_str(item) -> str:
        try:
            return item.get_name()
        except AttributeError:
            pass
        except TypeError:
            pass
        try:
            return item.get_value()
        except AttributeError:
            pass
        except TypeError:
            pass
        return str(item)

    @classmethod
    def get_enum_name(cls) -> str:
        return cls.__name__

    def __eq__(self, other):
        other_str = self._get_str(other)
        return other_str == self.get_name() or other_str == self.get_value()

    def __str__(self):
        return '{}.{}'.format(self.__class__.__name__, self.get_name())

    def __repr__(self):
        return "<{}.{}: '{}'>".format(self.__class__.__name__, self.get_name(), self.get_value())

    def __hash__(self):
        return hash(self.get_value())

    def __lt__(self, other):
        if hasattr(other, 'get_value'):
            other = other.get_value()
        return self.get_value() < other


class DynamicEnum(EnumItem):
    _enum_prepared = dict()
    _enum_items = dict()
    _enum_default_item = dict()

    @classmethod
    def get_default(cls) -> EnumItem:
        return cls._enum_default_item.get(cls.get_enum_name())

    @classmethod
    def set_default(cls, item: Union[EnumItem, Name]):
        if not isinstance(item, cls):
            item = cls.convert(item)
        cls._enum_default_item[cls.get_enum_name()] = item

    @classmethod
    def get_enum_items(cls, check: bool = True) -> list:
        if check:
            msg = 'For {method} DynamicEnum {enum} must be prepared, run "{enum}.prepare()" before use other methods'
            assert cls.is_prepared(), msg.format(method='get_enum_items()', enum=cls.get_enum_name())
        enum_name = cls.get_enum_name()
        items = cls._enum_items.get(enum_name, list())
        if not items:
            cls._enum_items[enum_name] = items
        return items

    @classmethod
    def add_enum_item(cls, item: EnumItem) -> None:
        cls.get_enum_items(check=False).append(item)

    @classmethod
    def convert(
            cls,
            obj: Union[EnumItem, Name],
            default: Union[EnumItem, arg.Auto, None] = arg.AUTO,
            skip_missing: bool = False,
    ):
        assert cls.is_prepared(), 'DynamicEnum must be prepared before usage'
        if isinstance(obj, cls):
            return obj
        for string in cls._get_name_and_value(obj):
            instance = cls.find_instance(string)
            if instance:
                return instance
        default = arg.delayed_acquire(default, cls.get_default)
        if default:
            return cls.convert(default)
        elif not skip_missing:
            raise ValueError('item {} is not an instance of DynamicEnum {}'.format(obj, cls.get_enum_name()))

    @classmethod
    def find_instance(cls, instance) -> Optional[EnumItem]:
        if instance in cls.__dict__:
            return cls.__dict__[instance]
        else:
            for item in cls.get_enum_items():
                if instance in (item, item.get_name(), item.get_value(), str(item)):
                    return item

    @classmethod
    def __new__(cls, *args, **kwargs):
        if not cls.is_prepared():
            obj = super().__new__(cls)
            obj.__init__(*args[1:], **kwargs)
            return obj
        else:
            try:
                return cls.convert(*args[1:], **kwargs)
            except ValueError as e:
                str_args = arg.get_str_from_args_kwargs(*args, **kwargs)
                msg = '{}({}): {}'.format(cls.__name__, str_args, e)
                raise ValueError(msg)

    @classmethod
    def is_prepared(cls) -> bool:
        return cls._enum_prepared.get(cls.get_enum_name(), False)

    @classmethod
    def set_prepared(cls, value: bool = True) -> None:
        cls._enum_prepared[cls.get_enum_name()] = value

    @classmethod
    def prepare(cls) -> Type:
        dict_copy = cls.__dict__.copy()
        for name, value in dict_copy.items():
            if isinstance(value, (str, int, arg.Auto)) and not cls._is_aux_name(name):
                item = cls(name, value)
                cls.add_enum_item(item)
                setattr(cls, name, item)
                if value == arg.AUTO:
                    cls.set_default(item)
        cls.set_prepared(True)
        return cls

    @staticmethod
    def _is_aux_name(name: str) -> bool:
        return name in AUX_NAMES or name.startswith('_')

    @staticmethod
    def _get_name_and_value(item) -> Iterable:
        if hasattr(item, 'get_name'):
            yield item.get_name()
        if hasattr(item, 'get_value'):
            yield item.get_value()
        else:
            yield str(item)


class ClassType(DynamicEnum):
    _dict_classes = dict()

    @classmethod
    def get_dict_classes(cls) -> dict:
        return cls._dict_classes

    @classmethod
    def set_dict_classes(cls, dict_classes: dict, skip_missing: bool = False, check: bool = True) -> None:
        if not cls.is_prepared():
            cls.prepare()
        cls._dict_classes = dict()
        cls.add_dict_classes(dict_classes, skip_missing=skip_missing, check=check)

    @classmethod
    def add_dict_classes(cls, dict_classes: dict, skip_missing: bool = False, check: bool = True) -> None:
        for name, class_obj in dict_classes.items():
            if check:
                assert isclass(class_obj), 'class expected, got {} as {}'.format(class_obj, type(class_obj))
            item = cls.convert(name, skip_missing=skip_missing, default=None)
            if item:
                cls._dict_classes[item] = class_obj

    @classmethod
    def add_classes(cls, *args, **kwargs) -> None:
        dict_classes = cls.get_dict_classes()
        for c in args:
            dict_classes[c.__name__] = c
        for n, c in kwargs.items():
            dict_classes[n] = c

    def get_class(self, default: Union[Optional[Class], Name] = None, skip_missing: bool = False) -> Class:
        dict_classes = self.get_dict_classes()
        assert dict_classes, 'classes must be defined by set_dict_classes() method'
        found_class = dict_classes.get(self)
        if found_class:
            return found_class
        elif default:
            if isclass(default):
                return default
            else:
                return self.convert(default).get_class(skip_missing=skip_missing)
        elif skip_missing:
            default = self.get_default()
            if hasattr(default, 'get_class'):
                return default.get_class(skip_missing=skip_missing)
        raise ValueError('class for {} not supported'.format(self))

    def build(self, *args, **kwargs):
        builder = self.get_class()
        if builder:
            return builder(*args, **kwargs)

    def isinstance(self, obj, by_type: bool = True) -> Optional[bool]:
        if by_type and hasattr(obj, 'get_type'):
            return obj.get_type() == self
        else:
            native_class = self.get_class(skip_missing=True)
            if native_class:
                return isinstance(obj, native_class)

    @classmethod
    def detect(cls, obj, default: Union[Optional[DynamicEnum], Name] = None) -> EnumItem:
        for item in cls.get_enum_items():
            assert isinstance(item, ClassType), '{} expected, got {} as {}'.format(cls.__name__, item, type(item))
            if item.isinstance(obj):
                return item
        if default:
            return default
        else:
            return cls.get_default()


class SubclassesType(ClassType):
    @classmethod
    def set_dict_subclasses(cls, dict_subclasses: dict, skip_missing: bool = False) -> None:
        super().set_dict_classes(dict_classes=dict_subclasses, skip_missing=skip_missing, check=False)

    @classmethod
    def set_dict_classes(cls, dict_classes: dict, skip_missing: bool = False, check: bool = True) -> None:
        dict_subclasses = dict()
        for k, v in dict_classes.items():
            if not isinstance(v, (list, tuple)):
                v = [v]
            dict_subclasses[k] = v
        cls.set_dict_subclasses(dict_subclasses)

    def get_subclasses(self, default: Union[Optional[Class], Name] = None, skip_missing: bool = False) -> Iterable:
        subclasses = super().get_class(default=default, skip_missing=skip_missing)
        assert isinstance(subclasses, Iterable)
        return subclasses

    def get_class(self, default: Union[Optional[Class], Name] = None, skip_missing: bool = False) -> Class:
        subclasses = self.get_subclasses()
        if isinstance(subclasses, (list, tuple)):
            return subclasses[0]
        elif default:
            return default
        elif not skip_missing:
            raise ValueError('class for {} not found'.format(self))
