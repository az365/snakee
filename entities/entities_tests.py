try:  # Assume we're a submodule in a package.
    from entities.graphs import graph_test
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .graphs import graph_test


def main():
    graph_test.main()


if __name__ == '__main__':
    main()
