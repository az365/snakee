from abc import ABC
from typing import Optional, Callable, Iterable, Generator, Union, Any
from inspect import getfullargspec

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.functions.arguments import get_name, get_list, get_str_from_args_kwargs
    from base.constants.chars import EMPTY, PLUS, MINUS, CROSS, COVERT, PROTECTED, DEFAULT_STR
    from base.interfaces.base_interface import BaseInterface, AutoOutput, CROP_SUFFIX, DEFAULT_LINE_LEN
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import Auto, AUTO
    from ..functions.arguments import get_name, get_list, get_str_from_args_kwargs
    from ..constants.chars import EMPTY, PLUS, MINUS, CROSS, COVERT, PROTECTED, DEFAULT_STR
    from ..interfaces.base_interface import BaseInterface, AutoOutput, CROP_SUFFIX, DEFAULT_LINE_LEN

Native = BaseInterface
OptionalFields = Union[str, Iterable, None]


class AbstractBaseObject(BaseInterface, ABC):
    def set_props(self, inplace: bool, **kwargs) -> Native:
        if inplace:
            return self.set_inplace(**kwargs)
        else:
            return self.set_outplace(**kwargs)

    def set_inplace(self, **kwargs) -> Native:
        for k, v in kwargs.items():
            method_name = 'set_{prop}'.format(prop=k)
            try:
                method = self.__getattribute__(method_name)
                method(v, inplace=True)
            except AttributeError as e:
                protected_name = PROTECTED + k
                if k in self.__dict__:
                    self.__dict__[k] = v
                elif protected_name in self.__dict__:
                    self.__dict__[protected_name] = v
                else:
                    raise AttributeError(e)
        return self

    def set_outplace(self, **kwargs) -> Native:
        props = self.get_props()
        props.update(kwargs)
        if 'check' in props:
            props['check'] = False
        return self.__class__(**props)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return dict()

    @classmethod
    def _get_key_member_names(cls):
        return list()

    def get_key_member_values(self) -> list:
        key_meta = list()
        for field in self._get_key_member_names():
            value = self.__dict__.get(field)
            key_meta.append(get_name(value))
        return key_meta

    @classmethod
    def _get_meta_field_by_member_name(cls, name: str) -> str:
        name = cls._get_meta_member_mapping().get(name, name)
        if name.startswith(PROTECTED):
            name = name[1:]
        return name

    @classmethod
    def _get_data_member_names(cls) -> tuple:
        return tuple()

    @classmethod
    def _get_data_fields_list(cls) -> list:
        return [cls._get_meta_field_by_member_name(m) for m in cls._get_data_member_names()]

    def _get_data_member_items(self) -> Iterable:
        for k in self._get_data_member_names():
            yield self.__dict__[k]

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return list()

    @classmethod
    def get_meta_fields_list(cls) -> list:
        return [cls._get_meta_field_by_member_name(k) for k in cls._get_meta_member_names()]

    @staticmethod
    def _get_other_meta_fields_list(other: Union[Native, Auto] = AUTO) -> tuple:
        if other == AUTO:
            return tuple()
        elif isinstance(other, AbstractBaseObject) or hasattr(other, '_get_init_args'):
            other_meta = other._get_init_args()
        elif hasattr(other, 'get_meta_fields_list'):
            other_meta = other.get_meta_fields_list()
        elif hasattr(other, 'get_meta'):
            other_meta = tuple(other.get_meta())
        else:
            other_meta = tuple()
        return other_meta

    def _meta_member_items(self) -> Generator:
        for k in self._get_meta_member_names():
            yield k, self.__dict__[k]

    def get_props(self, ex: OptionalFields = None, check: bool = True) -> dict:
        props = dict()
        ex_list = get_list(ex)
        for k, v in self.__dict__.items():
            k = self._get_meta_field_by_member_name(k)
            if k in ex_list:
                ex_list.remove(k)
            else:
                props[k] = v
        if check:
            assert not ex_list, 'get_props() got unexpected fields: {}'.format(ex_list)
        return props

    def get_meta(self, ex: OptionalFields = None) -> dict:
        ex_list = get_list(ex)
        ex_list += self._get_data_fields_list()
        meta = self.get_props(ex=ex_list)
        return meta

    def set_meta(self, inplace: bool = False, safe: bool = True, **meta) -> Native:
        if inplace:
            current_meta = self.get_meta()
            current_meta.update(meta)
            return self.set_inplace(**current_meta) or self
        else:
            if safe:
                meta = self._get_safe_meta(**meta)
            return self.__class__(*self._get_data_member_items(), **meta)

    def update_meta(self, inplace: bool = False, safe: bool = True, **meta) -> Native:
        current_meta = self.get_meta()
        current_meta.update(meta)
        if safe:
            current_meta = self._get_safe_meta(**current_meta)
        if inplace:
            return self.set_inplace(**current_meta) or self
        else:
            return self.__class__(*self._get_data_member_items(), **current_meta)

    def fill_meta(self, check: bool = True, **meta) -> Native:
        old_meta = self.get_meta()
        new_meta = meta.copy()
        new_meta.update(old_meta)
        if check:
            unsupported = [k for k in meta if k not in old_meta]
            assert not unsupported, 'class {} does not support these properties: {}'.format(
                self.__class__.__name__,
                unsupported,
            )
        for key, value in new_meta.items():
            if value is None or value == AUTO:
                new_meta[key] = old_meta.get(key)
        return self.__class__(*self._get_data_member_items(), **new_meta)

    def get_compatible_meta(self, other=AUTO, ex: OptionalFields = None, **kwargs) -> dict:
        other_meta = self._get_other_meta_fields_list(other)
        compatible_meta = dict()
        for k, v in list(self.get_meta(ex=ex).items()) + list(kwargs.items()):
            if k in other_meta:
                compatible_meta[k] = v
        return compatible_meta

    def get_meta_items(self, meta: Union[dict, Auto] = AUTO, ex: OptionalFields = None) -> Generator:
        meta = Auto.delayed_acquire(meta, self.get_meta)
        for k in self.get_ordered_meta_names(meta, ex=ex):
            yield k, meta[k]

    def get_ordered_meta_names(self, meta: Union[dict, Auto] = AUTO, ex: OptionalFields = None) -> Generator:
        meta = Auto.delayed_acquire(meta, self.get_meta)
        ex_list = get_list(ex)
        args = self._get_init_args()
        for k in args:
            if k in meta and k not in ex_list:
                yield k
        for k in meta:
            if k not in args and k not in ex_list:
                yield k

    @classmethod
    def _get_safe_meta(cls, **meta) -> dict:
        return {k: v for k, v in meta.items() if k in cls._get_init_args()}

    @classmethod
    def _get_init_args(cls) -> list:
        return getfullargspec(cls.__init__).args or list()

    def _get_init_defaults(self) -> dict:
        args = self._get_init_args()
        defaults = getfullargspec(self.__init__).defaults or list()
        no_default_count = len(args) - len(defaults)
        return dict(zip(args[no_default_count:], defaults))

    def _get_init_types(self) -> dict:
        return getfullargspec(self.__init__).annotations

    @staticmethod
    def _get_covert_props() -> tuple:
        return tuple()

    def _get_meta_args(self) -> list:
        return [self.__dict__[k] for k in self._get_key_member_names()]

    def _get_meta_kwargs(self, except_covert: bool = False, ex: OptionalFields = None) -> dict:
        meta_kwargs = self.get_meta(ex=ex).copy()
        for f in self._get_key_member_names():
            meta_kwargs.pop(self._get_meta_field_by_member_name(f), None)
        if except_covert:
            for f in self._get_covert_props():
                if meta_kwargs.get(f):
                    meta_kwargs[f] = COVERT
        return meta_kwargs

    def get_meta_defaults(self, ex: OptionalFields = None) -> Generator:
        meta = self.get_meta(ex=ex)
        args = getfullargspec(self.__init__).args
        defaults = getfullargspec(self.__init__).defaults
        for k, v in zip(args, defaults):
            if k in meta:
                yield k, v
        for k in meta:
            if k not in args:
                yield None

    def get_meta_records(self, ex: OptionalFields = None) -> Generator:
        init_defaults = self._get_init_defaults()
        for key, value in self.get_meta_items(ex=ex):
            actual_type = type(value).__name__
            expected_type = self._get_init_types().get(key)
            if hasattr(expected_type, '__name__'):
                expected_type = expected_type.__name__
            else:
                expected_type = str(expected_type).replace('typing.', EMPTY)
            default = init_defaults.get(key)
            if hasattr(value, 'get_caption'):
                caption = value.get_caption()
            else:
                caption = EMPTY
            yield dict(
                key=key,
                obj=value,
                caption=caption,
                value=self._get_value_repr(value),
                default=self._get_value_repr(default),
                actual_type=actual_type,
                expected_type=expected_type or DEFAULT_STR,
                defined=MINUS if value == default else PLUS if Auto.is_defined(value) else CROSS,
            )

    def get_str_meta(self) -> str:
        args_str = [i.__repr__() for i in self._get_meta_args()]
        meta_kwargs = self._get_meta_kwargs(except_covert=True)
        return get_str_from_args_kwargs(*args_str, **meta_kwargs, kwargs_order=self.get_ordered_meta_names())

    def get_detailed_repr(self) -> str:
        return '{}({})'.format(self.__class__.__name__, self.get_str_meta())

    def get_one_line_repr(
            self,
            str_meta: Union[str, Auto, None] = AUTO,
            max_len: int = DEFAULT_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        template = '{cls}({meta})'
        class_name = self.__class__.__name__
        str_meta = Auto.delayed_acquire(str_meta, self.get_str_meta)
        one_line_repr = template.format(cls=class_name, meta=str_meta)
        full_line_len = len(one_line_repr)
        if full_line_len > max_len:
            exceeded_len = full_line_len + len(crop) - max_len
            str_meta = str_meta[:-exceeded_len]
            one_line_repr = template.format(cls=class_name, meta=str_meta + crop)
        return one_line_repr

    def make_new(self, *args, ex: OptionalFields = None, safe: bool = True, **kwargs) -> Native:
        meta = self.get_meta(ex=ex)
        meta.update(kwargs)
        if safe:
            meta = self._get_safe_meta(**meta)
        return self.__class__(*args, **meta)

    @staticmethod
    def _get_value_repr(value: Any, default: str = DEFAULT_STR) -> str:
        if isinstance(value, Callable):
            return get_name(value, or_callable=False)
        elif value is not None:
            return repr(value)
        else:
            return default

    def describe(
            self,
            show_header: bool = True,
            comment: Optional[str] = None,
            depth: int = 1,
            output: AutoOutput = AUTO,
    ):
        detailed_repr = self.get_detailed_repr()
        if hasattr(output, 'output_line'):
            output_method = output.output_line
        else:
            output_method = print
        output_method(detailed_repr)
        output_method(comment)

    def __repr__(self):
        return self.get_detailed_repr()

    def __str__(self):
        return self.get_one_line_repr()

    def __eq__(self, other):
        if isinstance(other, BaseInterface) or hasattr(other, 'get_key_member_values'):
            return self.get_key_member_values() == other.get_key_member_values()
