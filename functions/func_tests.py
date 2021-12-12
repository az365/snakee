try:  # Assume we're a sub-module in a package.
    from functions.tests import test_dates
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .tests import test_dates


def main():
    test_dates.main()


if __name__ == '__main__':
    main()
