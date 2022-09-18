from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from interfaces import ItemType, ConnType, Name, Struct, AutoBool, Auto, AUTO
    from functions.secondary import item_functions as fs
    from streams.regular.any_stream import AnyStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import ItemType, ConnType, Name, Struct, AutoBool, Auto, AUTO
    from ...functions.secondary import item_functions as fs
    from .any_stream import AnyStream

EXPECTED_ITEM_TYPE = ItemType.Line


class LineStream(AnyStream):
    @deprecated_with_alternative('RegularStream(item_type=ItemType.Line)')
    def __init__(
            self,
            data: Iterable,
            name: str = AUTO,
            caption: str = '',
            item_type: ItemType = EXPECTED_ITEM_TYPE,
            struct: Struct = None,
            source=None,
            context=None,
            count=None,
            less_than=None,
            max_items_in_memory=AUTO,
            tmp_files=AUTO,
            check: bool = True,
    ):
        assert item_type == EXPECTED_ITEM_TYPE, f'got {item_type}'
        super().__init__(
            data=data, check=check,
            name=name, caption=caption,
            item_type=item_type, struct=struct,
            source=source, context=context,
            count=count, less_than=less_than,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def get_default_item_type() -> ItemType:
        return EXPECTED_ITEM_TYPE

    @classmethod
    @deprecated_with_alternative('connectors.ColumnFile().to_stream()')
    def from_text_file(
            cls, filename: Name,
            skip_first_line: bool = False, max_count: Optional[int] = None, expected_count: Union[Auto, int] = AUTO,
            check: AutoBool = AUTO, verbose: bool = False, step: Union[Auto, int] = AUTO,
    ):
        build_file = ConnType.LocalFile.get_class()
        sm_lines = build_file(
            filename,
            expected_count=expected_count, verbose=verbose,
        ).to_line_stream(
            check=check, step=step,
        )
        is_inherited = sm_lines.get_stream_type() != cls.__name__
        if is_inherited:
            sm_lines = sm_lines.map_to_type(function=fs.same(), stream_type=cls.__name__)
        if skip_first_line:
            sm_lines = sm_lines.skip(1)
        if max_count:
            sm_lines = sm_lines.take(max_count)
        return sm_lines

    @deprecated_with_alternative('RegularStream.write_to')
    def lazy_save(
            self, filename: Name, end: str = '\n',
            check: AutoBool = AUTO, verbose: bool = True, immediately: bool = False,
    ):
        def write_and_yield(fh, lines):
            n = 0
            for n, i in enumerate(lines):
                if n > 0:
                    fh.write(end)
                fh.write(str(i))
                yield i
            fh.close()
            self.log('Done. {} rows has written into {}'.format(n + 1, filename), verbose=verbose)
        if immediately:
            return self.to_text_file(filename, end=end, check=check, verbose=verbose, return_stream=True)
        else:
            fileholder = open(filename, 'w', encoding='utf8')
            items = write_and_yield(fileholder, self.get_items())
            return LineStream(items, **self.get_meta())

    @deprecated_with_alternative('RegularStream.write_to()')
    def to_text_file(
            self, filename: Name, end: str = '\n',
            check: AutoBool = AUTO, verbose: bool = True, return_stream: bool = True
    ):
        saved_stream = self.lazy_save(filename, end=end, verbose=False, immediately=False)
        if verbose:
            message = 'Writing {}'.format(filename)
            saved_stream = saved_stream.progress(expected_count=self.get_count(), message=message)
        saved_stream.pass_items()
        meta = self.get_static_meta()
        if return_stream:
            return self.from_text_file(filename, check=check, verbose=verbose).update_meta(**meta)
