try:  # Assume we're a sub-module in a package.
    from utils.tests import (
        test_dates,
        tests_mappers,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils.tests import (
        test_dates,
        tests_mappers,
    )


def main():
    test_dates.main()
    tests_mappers.main()


if __name__ == '__main__':
    main()
