try:  # Assume we're a sub-module in a package.
    from functions.tests import (
        test_dates,
        test_text,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .tests import (
        test_dates,
        test_text,
    )


def main():
    test_dates.main()
    test_text.main()


if __name__ == '__main__':
    main()
