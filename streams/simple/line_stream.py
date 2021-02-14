import sys
import json
import csv
import gzip as gz

try:
    from streams import stream_classes as sm
    from connectors import connector_classes as cs
    from utils import arguments as arg
    from functions import all_functions as fs
except ImportError:
    from .. import stream_classes as sm
    from ...connectors import connector_classes as cs
    from ...utils import arguments as arg
    from ...functions import all_functions as fs


max_int = sys.maxsize
while True:  # To prevent _csv.Error: field larger than field limit (131072)
    try:  # decrease the max_int value by factor 10 as long as the OverflowError occurs.
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)


def is_line(line):
    return isinstance(line, str)


def check_lines(lines, skip_errors=False):
    for i in lines:
        if is_line(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_lines(): this item is not a line: {}'.format(i))
        yield i


class LineStream(sm.AnyStream):
    def __init__(
            self,
            data,
            count=None,
            less_than=None,
            check=True,
            source=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
            context=None,
    ):
        super().__init__(
            check_lines(data) if check else data,
            count=count,
            less_than=less_than,
            source=source,
            context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.check = check

    @staticmethod
    def is_valid_item(item):
        return is_line(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_lines(items, skip_errors)

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
        ).set_meta(
            count=self.count,
        )

    @classmethod
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
        sm_lines = cs.TextFile(
            filename,
            encoding=encoding,
            gzip=gzip,
            expected_count=expected_count,
            verbose=verbose,
        ).to_lines_stream(
            check=check,
            step=step,
        )
        is_inherited = sm_lines.get_stream_type() != cls.__name__
        if is_inherited:
            sm_lines = sm_lines.map(function=fs.same(), to=cls.__name__)
        if skip_first_line:
            sm_lines = sm_lines.skip(1)
        if max_count:
            sm_lines = sm_lines.take(max_count)
        return sm_lines

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
            saved_stream = saved_stream.progress(expected_count=self.count, message=message)
        saved_stream.pass_items()
        meta = self.get_meta_except_count()
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

    def to_rows(self, delimiter=None):
        lines = self.get_items()
        rows = csv.reader(lines, delimiter=delimiter) if delimiter else csv.reader(lines)
        return sm.RowStream(
            rows,
            self.count,
        )

    def to_pairs(self, delimiter=None):
        lines = self.get_items()
        rows = csv.reader(lines, delimiter=delimiter) if delimiter else csv.reader(lines)
        return sm.RowStream(
            rows,
            self.count,
        ).to_pairs()
