try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        selection as sf,
    )
    from streams import stream_classes as sm
    from loggers import logger_classes as log
    from schema import schema_classes as sh
    from functions import all_functions as fs
    from selection import selection_classes as sn
    from loggers.logger_classes import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        selection as sf,
    )
    from .. import stream_classes as sm
    from ...loggers import logger_classes as log
    from ...schema import schema_classes as sh
    from ...functions import all_functions as fs
    from ...selection import selection_classes as sn
    from ...loggers.logger_classes import deprecated_with_alternative


DATA_MEMBERS = ['data', 'schema']
NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2  # schema fields


def is_row(row):
    return isinstance(row, (list, tuple))


def is_valid(row, schema):
    if is_row(row):
        if isinstance(schema, sh.SchemaDescription):
            return schema.is_valid_row(row)
        elif isinstance(schema, (list, tuple)):
            types = list()
            default_type = str
            for description in schema:
                field_type = description[TYPE_POS]
                if field_type in fs.DICT_CAST_TYPES.values():
                    types.append(field_type)
                else:
                    types.append(fs.DICT_CAST_TYPES.get(field_type, default_type))
            for value, field_type in zip(row, types):
                if not isinstance(value, field_type):
                    return False
            return True
        elif schema is None:
            return True


def check_rows(rows, schema, skip_errors=False):
    for r in rows:
        if is_valid(r, schema=schema):
            pass
        elif skip_errors:
            continue
        else:
            schema_str = schema.get_schema_str() if isinstance(schema, sh.SchemaDescription) else schema
            raise TypeError('check_records(): this item is not valid record for schema {}: {}'.format(schema_str, r))
        yield r


# @deprecated_with_alternative('schema.SchemaRow()')
def apply_schema_to_row(row, schema, skip_bad_values=False, logger=None):
    if isinstance(schema, sh.SchemaDescription) or hasattr(schema, 'get_converters'):
        converters = schema.get_converters('str', 'py')
        return [converter(value) for value, converter in zip(row, converters)]
    elif isinstance(schema, (list, tuple)):
        for c, (value, description) in enumerate(zip(row, schema)):
            field_type = description[TYPE_POS]
            try:
                cast_function = fs.cast(field_type)
                new_value = cast_function(value)
            except ValueError as e:
                field_name = description[NAME_POS]
                if logger:
                    message = 'Error while casting field {} ({}) with value {} into type {}'.format(
                        field_name, c,
                        value, field_type,
                    )
                    logger.log(msg=message, level=log.LoggingLevel.Error.value)
                if skip_bad_values:
                    if logger:
                        message = 'Skipping bad value in row:'.format(list(zip(row, schema)))
                        logger.log(msg=message, level=log.LoggingLevel.Debug.value)
                    new_value = None
                else:
                    message = 'Error in row: {}...'.format(str(list(zip(row, schema)))[:80])
                    if logger:
                        logger.log(msg=message, level=log.LoggingLevel.Warning.value)
                    else:
                        log.get_logger().show(message)
                    raise e
            row[c] = new_value
        return row
    else:
        raise TypeError


class SchemaStream(sm.RowStream):
    def __init__(
            self,
            data, schema=None,
            name=arg.DEFAULT, check=True,
            count=None, less_than=None,
            source=None, context=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
    ):
        self.schema = schema or list()
        super().__init__(
            data=self.get_validated_items(data, schema=schema),
            name=name, check=False,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.schema = schema or list()

    @staticmethod
    def get_data_members():
        return DATA_MEMBERS

    @staticmethod
    def get_item_type():
        return sh.SchemaRow

    @classmethod
    def is_valid_item_type(cls, item):
        return super().is_valid_item_type(item)

    def is_valid_item(self, item):
        return is_valid(
            item,
            schema=self.schema,
        )

    def get_validated_items(self, items, schema=arg.DEFAULT, skip_errors=False, context=arg.NOT_USED):
        if schema == arg.DEFAULT:
            schema = self.get_schema()
        return check_rows(items, schema, skip_errors)

    @staticmethod
    def get_item_type():
        return sn.it.ItemType.SchemaRow

    def get_schema(self):
        return self.schema

    def set_schema(self, schema, check=True):
        return self.stream(
            check_rows(self.get_data(), schema=schema) if check else self.get_data(),
            schema=schema,
        )

    def get_schematized_rows(self, rows, schema=arg.DEFAULT, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        schema = arg.undefault(schema, self.get_schema())
        if isinstance(schema, sh.SchemaDescription):  # actual approach
            converters = schema.get_converters('str', 'py')
            for r in rows:
                converted_row = list()
                for value, converter in zip(r, converters):
                    converted_value = converter(value)
                    converted_row.append(converted_value)
                yield converted_row.copy()
        else:  # deprecated approach
            for r in rows:
                if skip_bad_rows:
                    try:
                        yield apply_schema_to_row(r, schema, False, logger=self if verbose else None)
                    except ValueError:
                        self.log(['Skip bad row:', r], verbose=verbose)
                else:
                    yield apply_schema_to_row(r, schema, skip_bad_values, logger=self if verbose else None)

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        return self.stream(
            self.get_schematized_rows(self.get_items()),
            schema=schema,
            count=None if skip_bad_rows else self.get_count(),
            check=False,
        )

    def get_columns(self):
        if isinstance(self.schema, sh.SchemaDescription):
            return self.schema.get_columns()
        elif isinstance(self.schema, (list, tuple)):
            return [c[0] for c in self.schema]

    def get_schema_rows(self):
        for r in self.get_items():
            yield sh.SchemaRow(r, self.get_schema(), check=False)

    def schematized_map(self, function, schema):
        return self.__class__(
            map(function, self.get_items()),
            count=self.get_count(),
            less_than=self.get_count() or self.get_estimated_count(),
            schema=schema,
        )

    def select(self, *args, **kwargs):
        selection_description = sn.SelectionDescription.with_expressions(
            *args, **kwargs,
            target_item_type=self.get_item_type(), input_item_type=self.get_item_type(),
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
        )
        selection_function = selection_description.get_mapper()
        output_schema = selection_description.get_output_schema()
        return self.schematized_map(
            function=selection_function,
            schema=output_schema,
        )

    def filter(self, *fields, **expressions):
        primitives = (str, int, float, bool)
        expressions_list = [(k, fs.equal(v) if isinstance(v, primitives) else v) for k, v in expressions.items()]
        expressions_list = list(fields) + expressions_list
        # selection_method = sf.value_from_schema_row if use_extended_method else sn.value_from_schema_row
        selection_method = sf.value_from_schema_row

        def filter_function(r):
            for f in expressions_list:
                if not selection_method(r, f):
                    return False
            return True
        result = self.stream(
            filter(filter_function, self.get_items()),
        )
        return result.to_memory() if self.is_in_memory() else result
