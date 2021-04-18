try:  # Assume we're a sub-module in a package.
    from context import SnakeeContext
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..context import SnakeeContext
    from ..schema import schema_classes as sh


def test_detect_schema_by_title_row():
    title_row = ('page_id', 'hits_count', 'conversion_rate')
    expected = 'page_id int, hits_count int, conversion_rate numeric'
    received = sh.detect_schema_by_title_row(title_row).get_schema_str('pg')
    assert received == expected


def test_local_file():
    file_name = 'test_file_tmp.tsv'
    data = [{'a': 1}, {'a': 2}]
    cx = SnakeeContext()
    test_folder = cx.conn(cx.ct.ConnType.LocalFolder, name='test_tmp', path='test_tmp')
    test_file = test_folder.file(file_name, schema=['a'])
    stream = cx.sm.RecordStream(data).to_file(test_file)
    assert stream.get_list() == data, 'test case 0'
    cx.forget_child(test_file)
    test_file = cx.get_job_folder().file(file_name, schema=['a']).set_types(a=int)
    stream = cx.sm.RecordStream(data).to_file(test_file)
    assert stream.get_list() == data, 'test case 1'


def main():
    test_detect_schema_by_title_row()
    test_local_file()


if __name__ == '__main__':
    main()
