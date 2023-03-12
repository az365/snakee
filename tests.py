try:  # Assume we're a submodule in a package.
    from entities import entities_tests
    from content import content_tests
    from connectors import conn_test
    from functions import func_tests
    from series import series_tests
    from streams import stream_tests
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .entities import entities_tests
    from .content import content_tests
    from .connectors import conn_test
    from .functions import func_tests
    from .series import series_tests
    from .streams import stream_tests


def main():
    entities_tests.main()
    content_tests.main()
    conn_test.main()
    func_tests.main()
    series_tests.main()
    stream_tests.main()


if __name__ == '__main__':
    main()
