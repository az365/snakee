try:  # Assume we're a sub-module in a package.
    from functions.tests import (
        test_dates,
        test_text,
        test_grouping,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .tests import (
        test_dates,
        test_text,
        test_grouping,
    )


def main():
    test_dates.main()
    test_text.main()
    # test_grouping.main()


if __name__ == '__main__':
    main()
