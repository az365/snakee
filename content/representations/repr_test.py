try:  # Assume we're a submodule in a package.
    from content.representations import repr_classes as rc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import repr_classes as rc

ASSERTION_TEMPLATE = 'Test case {}: {} != {}'


def test_boolean_repr():
    data = True, False, None, 'a', []

    test_case = 1
    bool_rc = rc.BooleanRepresentation()
    expected = ['Yes', 'No ', '-  ', 'Yes', 'No ']
    received = [bool_rc.format(i) for i in data]
    assert received == expected, ASSERTION_TEMPLATE.format(test_case, received, expected)

    test_case = 2
    bool_rc = rc.BooleanRepresentation('+', '-', default='?', align_right=True)
    expected = ['+', '-', '?', '+', '-']
    received = [bool_rc.format(i) for i in data]
    assert received == expected, ASSERTION_TEMPLATE.format(test_case, received, expected)

    test_case = 3
    bool_rc = rc.BooleanRepresentation('+', '-', default='?', min_len=3, align_right=True)
    expected = ['  +', '  -', '  ?', '  +', '  -']
    received = [bool_rc.format(i) for i in data]
    assert received == expected, ASSERTION_TEMPLATE.format(test_case, received, expected)


def test_string_repr():
    test_case = 'string'
    str_rc = rc.StringRepresentation(max_len=7)
    data = 'abc', 'a\tb\nc'
    expected = ['abc    ', 'a -> ..']
    received = [str_rc.format(i) for i in data]
    assert received == expected, ASSERTION_TEMPLATE.format(test_case, received, expected)


def test_numeric_repr():
    data = [1.2, 10.2, 1.234567, 1., 1, 10000000000000]

    test_case = 'int'
    num_rc = rc.NumericRepresentation(0)
    expected = ['1      ', '10     ', '1      ', '1      ', '1      ', '10000..']
    received = [num_rc.format(i) for i in data]
    assert received == expected, ASSERTION_TEMPLATE.format(test_case, received, expected)

    test_case = 'float'
    num_rc = rc.NumericRepresentation(4)
    expected = ['1.2    ', '10.2   ', '1.234..', '1.0    ', '1.0    ', '10000..']
    received = [num_rc.format(i) for i in data]
    assert received == expected, ASSERTION_TEMPLATE.format(test_case, received, expected)


def main():
    test_boolean_repr()
    test_string_repr()


if __name__ == '__main__':
    main()
