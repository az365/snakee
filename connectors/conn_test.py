from datetime import datetime

try:  # Assume we're a submodule in a package.
    from context import SnakeeContext
    from content.struct.flat_struct import FlatStruct, DialectType, AnyField
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..context import SnakeeContext
    from ..content.struct.flat_struct import FlatStruct, DialectType, AnyField


def test_detect_struct_by_title_row():
    title_row = ('page_id', 'hits_count', 'conversion_rate')
    expected = 'page_id int, hits_count int, conversion_rate numeric'
    received = FlatStruct.get_struct_detected_by_title_row(title_row).get_struct_str(DialectType.Postgres)
    assert received == expected, '{} != {}'.format(received, expected)


def test_local_file():
    file_name = 'test_file_tmp.tsv'
    data = [{'a': 1}, {'a': 2}]
    cx = SnakeeContext()
    job_folder = cx.find_job_folder(required_folders=['test_tmp'])
    test_folder = job_folder.folder('test_tmp')
    test_file = test_folder.file(file_name, struct=['a']).set_types(a=int)
    stream = cx.sm.RecordStream(data).to_file(test_file)
    received = stream.get_list()
    assert received == data, 'test case 0: {} != {}'.format(received, data)
    cx.forget_all_conns()
    test_file = cx.get_job_folder().file(file_name, struct=['a']).set_types(a=int)
    stream = cx.sm.RecordStream(data).to_file(test_file)
    received = stream.get_list()
    assert received == data, 'test case 1: {} != {}'.format(received, data)


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
    job_folder = cx.find_job_folder(required_folders=['test_tmp'])
    test_folder = job_folder.folder('test_tmp')
    src = test_folder.file('test_file_tmp.tsv')
    dst = test_folder.file('test_dst_tmp.tsv', struct=src.get_struct())
    job = cx.ct.Job('test_job')
    op_name = 'test_operation'
    operation = cx.ct.TwinSync(name=op_name, src=src, dst=dst, procedure=lambda s: s)
    job.add_operation(operation)
    assert list(job.get_operations().keys()) == [op_name]
    assert job.get_inputs()['src'].get_name() == 'test_file_tmp.tsv'
    assert job.get_outputs()['dst'].get_name() == 'test_dst_tmp.tsv'
    assert job.get_operation(op_name).has_inputs()
    job.run()
    expected = list(src.get_items())
    received = list(dst.get_items())
    assert received == expected, 'expected {}, got {}'.format(expected, received)
    assert job.get_operation(op_name).is_done()
    dst.remove()
    assert not job.is_done()


def test_table():
    test_rows = [
        (datetime(2022, 1, 2, 0, 0), 'A', 123.456),
        (datetime(2022, 1, 3, 0, 0), 'B', 987.654),
    ]
    test_records = [
        dict(time=datetime(2022, 1, 2), cat_name='A', sum=123.456),
        dict(time=datetime(2022, 1, 3), cat_name='B', sum=987.654),
    ]
    struct = FlatStruct([AnyField('time', str), AnyField('cat_name', str), AnyField('sum', float)])
    cx = SnakeeContext()
    test_db = cx.ct.DatabaseTestStub('test_stub', 'test_host', 5432, 'test_db')
    test_db.test_stub_response = test_rows
    table = test_db.table('test_schema.test_table', struct=struct)
    record_stream = table.to_record_stream()
    received_data = list(record_stream.get_items())
    expected_data = test_records
    assert received_data == expected_data, '{} != {}'.format(received_data, expected_data)


def main():
    test_detect_struct_by_title_row()
    test_local_file()
    test_take_credentials_from_file()
    test_job()
    test_table()


if __name__ == '__main__':
    main()
