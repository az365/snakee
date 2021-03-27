import json
import gzip as gz

try:
    from streams import stream_classes as sm
    from connectors import connector_classes as ct
    from functions import all_functions as fs
    from utils import (
        arguments as arg,
        items as it,
    )
    from utils.decorators import deprecated_with_alternative
except ImportError:
    from .. import stream_classes as sm
    from ...connectors import connector_classes as ct
    from ...functions import all_functions as fs
    from ...utils import (
        arguments as arg,
        items as it,
    )
    from ...utils.decorators import deprecated_with_alternative


class LineStream(sm.AnyStream):
    def __init__(
            self,
            data,
            name=arg.DEFAULT, check=True,
            count=None, less_than=None,
            source=None, context=None,
            max_items_in_memory=arg.DEFAULT,
            tmp_files_template=arg.DEFAULT,
            tmp_files_encoding=arg.DEFAULT,
    ):
        super().__init__(
            data,
            name=name, check=check,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )

    @staticmethod
    def get_item_type():
        return it.ItemType.Line

    @classmethod
    def is_valid_item_type(cls, item):
        return cls.get_item_type().isinstance(item)

    def parse_json(self, default_value=None, to='RecordStream'):
        if isinstance(to, str):
            to = sm.StreamType(to)

        def json_loads(line):
            try:
                return json.loads(line)
            except json.JSONDecodeError as err:
                if default_value is not None:
                    return default_value
                else:
                    raise json.JSONDecodeError(err.msg, err.doc, err.pos)
        return self.map(
            json_loads,
            to=to,
        )

    @classmethod
    @deprecated_with_alternative('*Stream.from_file')
    def from_text_file(
            cls,
            filename,
            encoding=None, gzip=False,
            skip_first_line=False, max_count=None,
            check=arg.DEFAULT,
            expected_count=arg.DEFAULT,
            verbose=False,
            step=arg.DEFAULT,
    ):
        sm_lines = ct.TextFile(
            filename,
            encoding=encoding,
            gzip=gzip,
            expected_count=expected_count,
            folder=ct.get_default_job_folder(),
            verbose=verbose,
        ).to_line_stream(
            check=check,
            step=step,
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
            self,
            filename,
            encoding=None, gzip=False,
            end='\n', check=arg.DEFAULT,
            verbose=True, immediately=False,
    ):
        def write_and_yield(fh, lines):
            n = 0
            for n, i in enumerate(lines):
                if n > 0:
                    fh.write(end.encode(encoding) if gzip else end)
                fh.write(str(i).encode(encoding) if gzip else str(i))
                yield i
            fh.close()
            self.log('Done. {} rows has written into {}'.format(n + 1, filename), verbose=verbose)
        if immediately:
            self.to_text_file(
                filename,
                encoding=encoding,
                end=end,
                check=check,
                verbose=verbose,
                return_stream=True,
            )
        else:
            if gzip:
                fileholder = gz.open(filename, 'w')
            else:
                fileholder = open(filename, 'w', encoding=encoding) if encoding else open(filename, 'w')
            return LineStream(
                write_and_yield(fileholder, self.get_items()),
                **self.get_meta()
            )

    @deprecated_with_alternative('*Stream.write_to()')
    def to_text_file(
            self,
            filename,
            encoding=None, gzip=False,
            end='\n', check=arg.DEFAULT,
            verbose=True, return_stream=True
    ):
        saved_stream = self.lazy_save(
            filename,
            encoding=encoding,
            gzip=gzip,
            end=end,
            verbose=False,
            immediately=False,
        )
        if verbose:
            message = ('Compressing gzip ito {}' if gzip else 'Writing {}').format(filename)
            saved_stream = saved_stream.progress(expected_count=self.get_count(), message=message)
        saved_stream.pass_items()
        meta = self.get_static_meta()
        if return_stream:
            return self.from_text_file(
                filename,
                encoding=encoding,
                gzip=gzip,
                check=check,
                verbose=verbose,
            ).update_meta(
                **meta
            )
