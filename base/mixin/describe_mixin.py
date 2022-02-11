from abc import ABC, abstractmethod
from typing import Optional

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoBool, AutoCount, Array, Columns
    from base.functions.arguments import get_str_from_args_kwargs
    from base.interfaces.data_interface import SimpleDataInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoBool, AutoCount, Array, Columns
    from ..functions.arguments import get_str_from_args_kwargs
    from ..interfaces.data_interface import SimpleDataInterface

Native = SimpleDataInterface

DEFAULT_SHOW_COUNT = 10


class DescribeMixin(ABC):
    def get_brief_repr(self) -> str:
        return "{}('{}')".format(self.__class__.__name__, self.get_name())

    def get_str_count(self, default: Optional[str] = '(iter)') -> Optional[str]:
        if hasattr(self, 'get_count'):
            count = self.get_count()
        else:
            count = None
        if Auto.is_defined(count):
            return str(count)
        else:
            return default

    def get_count_repr(self, default: str = '<iter>') -> str:
        count = self.get_str_count(default=default)
        if not Auto.is_defined(count):
            count = default
        return '{} items'.format(count)

    def get_shape_repr(self) -> str:
        len_repr = self.get_count_repr()
        if hasattr(self, 'get_column_repr'):
            column_repr = self.get_column_repr()
        else:
            column_repr = None
        dimensions_repr = list()
        if len_repr:
            dimensions_repr += len_repr
        if column_repr:
            dimensions_repr += column_repr
        return ', '.join(dimensions_repr)

    def get_one_line_repr(self) -> str:
        description_args = list()
        name = self.get_name()
        if name:
            description_args.append(name)
        if self.get_str_count(default=None) is not None:
            description_args.append(self.get_shape_repr())
        return '{}({})'.format(self.__class__, get_str_from_args_kwargs(*description_args))

    def get_detailed_repr(self) -> str:
        return '{}({})'.format(self.__class__.__name__, self.get_str_meta())

    def show(
            self,
            count: int = DEFAULT_SHOW_COUNT,
            message: Optional[str] = None,
            filters: Columns = None,
            columns: Columns = None,
            actualize: AutoBool = Auto,
            as_dataframe: AutoBool = Auto,
            **kwargs
    ):
        if hasattr(self, 'actualize'):
            if Auto.is_auto(actualize):
                self.actualize(if_outdated=True)
            elif actualize:
                self.actualize(if_outdated=False)
        return self.to_record_stream(message=message).show(
            count=count, as_dataframe=as_dataframe,
            filters=filters or list(), columns=columns,
        )

    def describe(
            self,
            *filter_args,
            count: Optional[int] = DEFAULT_SHOW_COUNT,
            columns: Optional[Array] = None,
            show_header: bool = True,
            struct_as_dataframe: bool = False,
            safe_filter: bool = True,
            actualize: AutoBool = AUTO,
            **filter_kwargs
    ):
        if show_header:
            for line in self.get_str_headers():
                self.log(line)
        example_item, example_stream, example_comment = dict(), None, ''
        if self.is_existing():
            if Auto.acquire(actualize, not self.is_actual()):
                self.actualize()
            if self.is_empty():
                message = '[EMPTY] file is empty, expected {} columns:'.format(self.get_column_count())
            else:
                message = self.get_validation_message()
                example_tuple = self._prepare_examples(safe_filter=safe_filter, filters=filter_args, **filter_kwargs)
                example_item, example_stream, example_comment = example_tuple
        else:
            message = '[NOT_EXISTS] file is not created yet, expected {} columns:'.format(self.get_column_count())
        if show_header:
            self.log('{} {}'.format(self.get_datetime_str(), message))
            if self.get_invalid_fields_count():
                self.log('Invalid columns: {}'.format(get_str_from_args_kwargs(*self.get_invalid_columns())))
            self.log('')
        struct = self.get_struct()
        struct_dataframe = struct.describe(
            as_dataframe=struct_as_dataframe, example=example_item,
            logger=self.get_logger(), comment=example_comment,
        )
        if struct_dataframe is not None:
            return struct_dataframe
        if example_stream and count:
            return self.show_example(
                count=count, example=example_stream,
                columns=columns, comment=example_comment,
            )
