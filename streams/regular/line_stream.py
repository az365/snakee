from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from interfaces import Stream, StreamInterface, StreamType, ItemType, ConnType, Name, Struct, AutoBool, Auto, AUTO
    from functions.secondary import item_functions as fs
    from streams.regular.any_stream import AnyStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import Stream, StreamInterface, StreamType, ItemType, ConnType, Name, Struct, AutoBool, Auto, AUTO
    from ...functions.secondary import item_functions as fs
    from .any_stream import AnyStream


class LineStream(AnyStream):
    def __init__(
            self,
            data: Iterable,
            name: str = AUTO,
            caption: str = '',
            struct: Struct = None,
            check: bool = True,
            count=None,
            less_than=None,
            source=None,
            context=None,
            max_items_in_memory=AUTO,
            tmp_files=AUTO,
    ):
        super().__init__(
            data=data, struct=struct, check=check,
            name=name, caption=caption,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def get_item_type() -> ItemType:
        return ItemType.Line

    @classmethod
    def is_valid_item_type(cls, item) -> bool:
        return cls.get_item_type().isinstance(item)

    def parse_json(self, default_value=None, to: Union[StreamType, str] = StreamType.RecordStream) -> Stream:
        stream_type = StreamType.find_instance(to)
        assert isinstance(stream_type, StreamType)
        return self.map_to_type(fs.json_loads(default_value), stream_type=stream_type)

    def get_demo_example(self, count: int = 3) -> list:
        demo_example = super().get_demo_example(count=count)
        assert isinstance(demo_example, Iterable), 'get_demo_example(): Expected Iterable, got {}'.format(demo_example)
        return list(demo_example)

    @classmethod
    @deprecated_with_alternative('*Stream.from_file')
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
            sm_lines = sm_lines.map_to(function=fs.same(), stream_type=cls.__name__)
        if skip_first_line:
            sm_lines = sm_lines.skip(1)
        if max_count:
            sm_lines = sm_lines.take(max_count)
        return sm_lines

    @deprecated_with_alternative('*Stream.write_to')
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

    @deprecated_with_alternative('*Stream.write_to()')
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
