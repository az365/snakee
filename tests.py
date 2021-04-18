try:  # Assume we're a sub-module in a package.
    from streams import stream_tests
    from utils import utils_tests
    from fields import fields_test
    from series import series_tests
    from connectors import conn_test
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .streams import stream_tests
    from .utils import utils_tests
    from .fields import fields_test
    from .series import series_tests
    from .connectors import conn_test


def main():
    stream_tests.main()
    series_tests.main()
    utils_tests.main()
    fields_test.main()
    conn_test.main()


if __name__ == '__main__':
    main()
