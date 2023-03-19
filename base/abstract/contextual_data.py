from typing import Optional, Iterable, Generator, Sequence, Tuple, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Count, Array
    from base.functions.arguments import get_str_from_args_kwargs, get_generated_name
    from base.functions.errors import get_type_err_msg
    from base.interfaces.display_interface import DisplayInterface, DEFAULT_EXAMPLE_COUNT
    from base.interfaces.context_interface import ContextInterface
    from base.mixin.data_mixin import DataMixin, EMPTY, UNK_COUNT_STUB, DEFAULT_CHAPTER_TITLE_LEVEL
    from base.mixin.sourced_mixin import SourcedMixin
    from base.mixin.contextual_mixin import ContextualMixin
    from base.abstract.abstract_base import AbstractBaseObject
    from base.abstract.named import AbstractNamed
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Count, Array
    from ..functions.arguments import get_str_from_args_kwargs, get_generated_name
    from ..functions.errors import get_type_err_msg
    from ..interfaces.display_interface import DisplayInterface, DEFAULT_EXAMPLE_COUNT
    from ..interfaces.context_interface import ContextInterface
    from ..mixin.data_mixin import DataMixin, EMPTY, UNK_COUNT_STUB, DEFAULT_CHAPTER_TITLE_LEVEL
    from ..mixin.sourced_mixin import SourcedMixin
    from ..mixin.contextual_mixin import ContextualMixin
    from .abstract_base import AbstractBaseObject
    from .named import AbstractNamed

Native = Union[AbstractNamed, SourcedMixin, ContextualMixin, DataMixin]
Data = Any
Context = Optional[ContextInterface]
OptionalFields = Union[str, Iterable, None]
Display = Optional[DisplayInterface]

DATA_MEMBER_NAMES = '_data',
SPECIFIC_MEMBER_NAMES = '_source',
DYNAMIC_META_FIELDS = tuple()


class ContextualDataWrapper(AbstractNamed, SourcedMixin, ContextualMixin, DataMixin):
    def __init__(
            self,
            data: Data,
            name: Optional[str],
            caption: str = EMPTY,
            source: Native = None,
            context: Context = None,
            check: bool = True,
    ):
        self._data = data
        if name is None:
            name = get_generated_name(prefix=self.__class__.__name__)
        self._source = source
        super().__init__(name=name, caption=caption)
        if source is not None:
            self.register_in_source(check=check)
        if context is not None:
            self.set_context(context, reset=not check, inplace=True)

    @classmethod
    def _get_data_member_names(cls):
        return DATA_MEMBER_NAMES  # '_data',

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return super()._get_meta_member_names() + list(SPECIFIC_MEMBER_NAMES)

    @classmethod
    def _get_key_member_names(cls) -> list:
        return super()._get_key_member_names() + list(SPECIFIC_MEMBER_NAMES)

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

    def get_compatible_static_meta(self, other=None, ex=None, **kwargs) -> dict:
        meta = self.get_compatible_meta(other=other, ex=ex, **kwargs)
        for f in self._get_dynamic_meta_fields():
            meta.pop(f, None)
        return meta

    def get_str_count(self, default: str = UNK_COUNT_STUB) -> str:
        if hasattr(self, 'get_count'):
            count = self.get_count()
        else:
            count = None
        if count is not None:
            return str(count)
        else:
            return default

    # @deprecated
    def get_count_repr(self, default: str = UNK_COUNT_STUB) -> str:
        count = self.get_str_count()
        if count is None:
            count = default
        return f'{count} items'

    def _get_demo_records_and_columns(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            columns: Optional[Array] = None,
            filters: Optional[Array] = None,
            example: Optional[DataMixin] = None,
    ) -> Tuple[Sequence, Sequence]:
        if example is None:
            example = self._get_demo_example(count=count, columns=columns, filters=filters, example=example)
        elif not isinstance(example, DataMixin):
            msg = get_type_err_msg(expected=DataMixin, got=example, arg='example')
            raise TypeError(msg)
        if hasattr(example, 'get_columns') and hasattr(example, 'get_records'):  # RegularStream, SqlStream
            records = example.get_records()  # ConvertMixin.get_records(), SqlStream.get_records()
            columns = example.get_columns()  # StructMixin.get_columns(), RegularStream.get_columns()
        else:
            item_field = 'item'
            records = [{item_field: i} for i in example]
            columns = [item_field]
        return records, columns

    def get_description_items(
            self,
            comment: Optional[str] = None,
            depth: int = 1,
            count: Count = DEFAULT_EXAMPLE_COUNT,
            columns: Optional[Array] = None,
            actualize: Optional[bool] = None,
            safe_filter: bool = True,
            filters: Optional[Iterable] = None,
            named_filters: Optional[dict] = None,
            **kwargs
    ) -> Generator:
        assert not kwargs, f'{self.__class__.__name__}.describe(): kwargs not supported'
        yield self.get_display().get_header_chapter_for(self, level=1, comment=comment)
        if hasattr(self, '_prepare_examples_with_title'):  # isinstance(self, ValidateMixin)
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
        if depth > 0 and hasattr(self, 'get_struct_chapter'):  # isinstance(self, (StructMixin, ColumnarMixin)):
            yield self.get_struct_chapter(
                example_item=example_item, comment=example_comment,
                level=DEFAULT_CHAPTER_TITLE_LEVEL, name='Columns',
            )
        if example_stream and count and hasattr(self, 'get_example_chapter'):  # isinstance(self, ValidateMixin):
            yield self.get_example_chapter(
                count, columns=columns, example=example_stream, comment=example_comment,
                level=DEFAULT_CHAPTER_TITLE_LEVEL, name='Example',
            )
        if depth > 1:
            yield self.get_display().get_meta_chapter_for(self, level=DEFAULT_CHAPTER_TITLE_LEVEL, name='Meta')

    def describe(
            self,
            comment: Optional[str] = None,
            depth: int = 1,
            display: Display = None,
            count: Count = DEFAULT_EXAMPLE_COUNT,
            columns: Optional[Array] = None,
            actualize: Optional[bool] = None,
            safe_filter: bool = True,
            filters: Optional[Iterable] = None,
            named_filters: Optional[dict] = None,
            **kwargs
    ) -> Native:
        display = self.get_display(display)
        for i in self.get_description_items(
            comment=comment, depth=depth,
            count=count, columns=columns, actualize=actualize,
            filters=filters, named_filters=named_filters, safe_filter=safe_filter,
            **kwargs,
        ):
            if isinstance(i, str):
                display.append(i)
            else:  # isinstance(i, DocumentItem):
                display.display_item(i)
        return self
