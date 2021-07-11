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
    test_file = test_folder.file(file_name, schema=['a']).set_types(a=int)
    stream = cx.sm.RecordStream(data).to_file(test_file)
    assert stream.get_list() == data, 'test case 0'
    cx.forget_child(test_file)
    test_file = cx.get_job_folder().file(file_name, schema=['a']).set_types(a=int)
    stream = cx.sm.RecordStream(data).to_file(test_file)
    assert stream.get_list() == data, 'test case 1'


def test_take_credentials_from_file():
    file_name = 'test_creds.txt'
    data = [
        ('ch', 'test_login_ch', 'test_pass_ch'),
        ('pg', 'test_login_pg', 'test_pass_pg'),
        ('s3', 'test_login_s3', 'test_pass_s3'),
    ]
    cx = SnakeeContext()
    test_file = cx.get_job_folder().file(file_name)
    test_file.write_lines(map(lambda r: '\t'.join(r), data))
    test_db = cx.ct.PostgresDatabase('pg', 'pg_host', 5432, 'pg_db')
    cx.take_credentials_from_file(test_file)
    expected = data[1][1:]
    received = test_db.get_credentials()
    assert received == expected


def test_job():
    cx = SnakeeContext()
    src = cx.get_job_folder().file('test_file_tmp.tsv')
    dst = cx.get_job_folder().file('test_dst_tmp.tsv', schema=src.get_schema())
    job = cx.ct.Job('test_job')
    op_name = 'test_operation'
    operation = cx.ct.TwinSync(name=op_name, src=src, dst=dst, procedure=lambda s: s)
    job.add_operation(operation)
    assert list(job.get_operations().keys()) == [op_name]
    assert job.get_inputs()['src'].get_name() == 'test_file_tmp.tsv'
    assert job.get_outputs()['dst'].get_name() == 'test_dst_tmp.tsv'
    assert job.get_operation(op_name).has_inputs()
    job.run()
    assert list(src.get_data()) == list(dst.get_data())
    assert job.get_operation(op_name).is_done()
    dst.remove()
    assert not job.is_done()


def main():
    test_detect_schema_by_title_row()
    test_local_file()
    test_take_credentials_from_file()
    test_job()


if __name__ == '__main__':
    main()
