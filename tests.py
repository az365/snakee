try:  # Assume we're a submodule in a package.
    from streams import stream_tests
    from content import content_tests
    from functions import func_tests
    from series import series_tests
    from connectors import conn_test
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .streams import stream_tests
    from .content import content_tests
    from .functions import func_tests
    from .series import series_tests
    from .connectors import conn_test


def main():
    stream_tests.main()
    series_tests.main()
    content_tests.main()
    func_tests.main()
    conn_test.main()


if __name__ == '__main__':
    main()
