try:  # Assume we're a sub-module in a package.
    from streams import stream_tests
    from series import series_tests
    from utils import utils_tests
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .streams import stream_tests
    from .series import series_tests
    from .utils import utils_tests


def main():
    stream_tests.main()
    series_tests.main()
    utils_tests.main()


if __name__ == '__main__':
    main()
