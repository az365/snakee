from . import field_classes as fc


def test_add_fields():
    field_a = fc.field('a', int)
    field_b = fc.field('b', float)
    expected0 = fc.group(field_a, field_b)
    received0 = field_a + field_b
    assert received0 == expected0, '{} != {}'.format(received0, expected0)
    field_c = fc.field('c', str)
    expected1 = fc.group(field_a, field_b, field_c)
    received1 = fc.group(field_a, field_b, field_c)
    assert received1 == expected1, '{} != {}'.format(received1, expected1)


def main():
    test_add_fields()


if __name__ == '__main__':
    main()
