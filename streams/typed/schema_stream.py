try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as fx
    from loggers import logger_classes
    from schema import schema_classes as sh
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as fx
    from ...loggers import logger_classes
    from ...schema import schema_classes as sh
    from ...functions import all_functions as fs


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


def apply_schema_to_row(row, schema, skip_bad_values=False, logger=None):
    if isinstance(schema, sh.SchemaDescription):
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
                    logger.log(msg=message, level=logger_classes.LoggingLevel.Error.value)
                if skip_bad_values:
                    if logger:
                        message = 'Skipping bad value in row:'.format(list(zip(row, schema)))
                        logger.log(msg=message, level=logger_classes.LoggingLevel.Debug.value)
                    new_value = None
                else:
                    message = 'Error in row: {}...'.format(str(list(zip(row, schema)))[:80])
                    if logger:
                        logger.log(msg=message, level=logger_classes.LoggingLevel.Warning.value)
                    else:
                        logger_classes.get_logger().show(message)
                    raise e
            row[c] = new_value
        return row
    else:
        raise TypeError


class SchemaFlux(fx.RowsFlux):
    def __init__(
            self,
            data,
            count=None,
            less_than=None,
            check=True,
            schema=None,
            source=None,
            context=None,
            max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=fx.TMP_FILES_TEMPLATE,
            tmp_files_encoding=fx.TMP_FILES_ENCODING,
    ):
        super().__init__(
            check_rows(data, schema) if check else data,
            count=count,
            less_than=less_than,
            check=check,
            source=source,
            context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.schema = schema or list()

    def is_valid_item(self, item):
        return is_valid(
            item,
            schema=self.schema,
        )

    def valid_items(self, items, skip_errors=False):
        return check_rows(
            items,
            self.schema,
            skip_errors,
        )

    def get_schema(self):
        return self.schema

    def set_schema(self, schema, check=True):
        return SchemaFlux(
            check_rows(self.data, schema=schema) if check else self.data,
            count=self.count,
            less_than=self.less_than,
            schema=schema,
        )

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        def apply_schema_to_rows(rows):
            if isinstance(schema, sh.SchemaDescription):
                converters = schema.get_converters('str', 'py')
                for r in rows:
                    converted_row = list()
                    for value, converter in zip(r, converters):
                        converted_value = converter(value)
                        converted_row.append(converted_value)
                    yield converted_row.copy()
            else:
                for r in rows:
                    if skip_bad_rows:
                        try:
                            yield apply_schema_to_row(r, schema, False, logger=self if verbose else None)
                        except ValueError:
                            self.log(['Skip bad row:', r], verbose=verbose)
                    else:
                        yield apply_schema_to_row(r, schema, skip_bad_values, logger=self if verbose else None)
        return SchemaFlux(
            apply_schema_to_rows(self.data),
            count=None if skip_bad_rows else self.count,
            less_than=self.less_than,
            check=False,
            schema=schema,
        )

    def get_columns(self):
        if isinstance(self.schema, sh.SchemaDescription):
            return self.schema.get_columns()
        elif isinstance(self.schema, (list, tuple)):
            return [c[0] for c in self.schema]
