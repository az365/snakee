from typing import Optional, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoBool, Count, Array
    from base.functions.arguments import get_str_from_args_kwargs
    from base.interfaces.display_interface import DEFAULT_EXAMPLE_COUNT
    from base.interfaces.context_interface import ContextInterface
    from base.mixin.data_mixin import DataMixin, EMPTY, UNK_COUNT_STUB, DEFAULT_CHAPTER_TITLE_LEVEL
    from base.mixin.contextual_mixin import ContextualMixin
    from base.abstract.abstract_base import AbstractBaseObject
    from base.abstract.sourced import Sourced, SourcedInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoBool, Count, Array
    from ..functions.arguments import get_str_from_args_kwargs
    from ..interfaces.display_interface import DEFAULT_EXAMPLE_COUNT
    from ..interfaces.context_interface import ContextInterface
    from ..mixin.data_mixin import DataMixin, EMPTY, UNK_COUNT_STUB, DEFAULT_CHAPTER_TITLE_LEVEL
    from ..mixin.contextual_mixin import ContextualMixin
    from .abstract_base import AbstractBaseObject
    from .sourced import Sourced, SourcedInterface

Native = Union[Sourced, ContextualMixin, DataMixin]
Data = Any
OptionalFields = Union[str, Iterable, None]
Source = Optional[SourcedInterface]
Context = Optional[ContextInterface]

DATA_MEMBER_NAMES = '_data',
DYNAMIC_META_FIELDS = tuple()


class ContextualDataWrapper(Sourced, ContextualMixin, DataMixin):
    def __init__(
            self,
            data: Data,
            name: str,
            caption: str = EMPTY,
            source: Source = None,
            context: Context = None,
            check: bool = True,
    ):
        self._data = data
        super().__init__(name=name, caption=caption, source=source, check=check)
        if Auto.is_defined(context):
            self.set_context(context, reset=not check, inplace=True)

    @classmethod
    def _get_data_member_names(cls):
        return DATA_MEMBER_NAMES  # '_data',

    def get_data(self) -> Data:
        return self._data

    def set_data(self, data: Data, inplace: bool, **kwargs) -> Native:
        if inplace:
            self._data = data
            return self.set_meta(**self.get_static_meta())
        else:
            return self.__class__(data, **self.get_static_meta())

    def apply_to_data(self, function, *args, dynamic=False, **kwargs):
        data = function(self._get_data(), *args, **kwargs)
        meta = self.get_static_meta() if dynamic else self.get_meta()
        return self.__class__(data=data, **meta)

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS  # empty (no meta fields)

    def get_static_meta(self, ex: OptionalFields = None) -> dict:
        meta = self.get_meta(ex=ex)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f, None)
        return meta

    def get_compatible_static_meta(self, other=AUTO, ex=None, **kwargs) -> dict:
        meta = self.get_compatible_meta(other=other, ex=ex, **kwargs)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f, None)
        return meta

    def get_str_count(self, default: str = UNK_COUNT_STUB) -> str:
        if hasattr(self, 'get_count'):
            count = self.get_count()
        else:
            count = None
        if Auto.is_defined(count):
            return str(count)
        else:
            return default

    # @deprecated
    def get_count_repr(self, default: str = UNK_COUNT_STUB) -> str:
        count = self.get_str_count()
        if not Auto.is_defined(count):
            count = default
        return f'{count} items'

    def get_description_items(
            self,
            count: Count = DEFAULT_EXAMPLE_COUNT,
            columns: Optional[Array] = None,
            comment: Optional[str] = None,
            safe_filter: bool = True,
            actualize: AutoBool = AUTO,
            filters: Optional[Iterable] = None,
            named_filters: Optional[dict] = None,
    ) -> Generator:
        yield self.get_display().get_header_chapter_for(self, level=1, comment=comment)
        if hasattr(self, '_prepare_examples_with_title'):  # isinstance(self, ValidateMixin):
            struct_title, example_item, example_stream, example_comment = self._prepare_examples_with_title(
                *filters or list(), **named_filters or dict(), safe_filter=safe_filter,
                example_row_count=count, actualize=actualize,
                verbose=False,
            )
            yield struct_title
        else:
            example_item = dict()
            example_stream = None
            example_comment = f'{repr(self)} has no example item(s)'
        if hasattr(self, 'get_invalid_columns'):  # isinstance(self, ValidateMixin):
            invalid_columns = self.get_invalid_columns()
        else:
            invalid_columns = None
        if invalid_columns:
            invalid_columns_str = get_str_from_args_kwargs(*invalid_columns)
            yield f'Invalid columns: {invalid_columns_str}'
        if hasattr(self, 'get_struct_chapter'):  # isinstance(self, (StructMixin, ColumnarMixin)):
            yield self.get_struct_chapter(
                example_item=example_item, comment=example_comment,
                level=DEFAULT_CHAPTER_TITLE_LEVEL, name='Columns',
            )
        if example_stream and count and hasattr(self, 'get_example_chapter'):  # isinstance(self, ValidateMixin):
            yield self.get_example_chapter(
                count, columns=columns, example=example_stream, comment=example_comment,
                level=DEFAULT_CHAPTER_TITLE_LEVEL, name='Example',
            )
        yield self.get_display().get_meta_chapter_for(self, level=DEFAULT_CHAPTER_TITLE_LEVEL, name='Meta')
