try:
    from fields import field_classes as fc
    from selection import concrete_expression as ce
except ImportError:
    from . import field_classes as fc
    from ..selection import concrete_expression as ce


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


def test_transfer_selection():
    expression_a = ce.TrivialDescription('field_a', target_item_type=ce.it.ItemType.Record)
    schema = fc.group(fc.field('field_a', float), fc.field('field_b', bool))
    assert expression_a.get_target_item_type() == ce.it.ItemType.Record
    assert expression_a.get_dict_output_field_types(schema) == {'field_a': fc.FieldType.Float}
    assert expression_a.get_value_from_item(dict(field_a=1.1, field_b=True)) == 1.1


def main():
    test_add_fields()
    test_transfer_selection()


if __name__ == '__main__':
    main()
