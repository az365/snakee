try:  # Assume we're a submodule in a package.
    from content.fields import fields_test
    from content.representations import repr_test
    from content.terms import terms_test
    from content.visuals import visuals_test
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .fields import fields_test
    from .representations import repr_test
    from .terms import terms_test
    from .visuals import visuals_test


def main():
    fields_test.main()
    repr_test.main()
    terms_test.main()
    visuals_test.main()


if __name__ == '__main__':
    main()
