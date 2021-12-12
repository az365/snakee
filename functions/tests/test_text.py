try:  # Assume we're a submodule in a package.
    from functions.primary import text as tx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..primary import text as tx


def test_norm_text():
    example = '\t Абв 123 Gb\n'
    expected = 'абв gb'
    received = tx.norm_text(example)
    assert received == expected


def main():
    test_norm_text()


if __name__ == '__main__':
    main()
