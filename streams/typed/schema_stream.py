try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from loggers import logger_classes as log
    from schema import schema_classes as sh
    from functions import all_functions as fs
    from selection import selection_classes as sn
    from utils import selection
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...loggers import logger_classes as log
    from ...schema import schema_classes as sh
    from ...functions import all_functions as fs
    from ...selection import selection_classes as sn
    from ...utils import selection


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
            data,
            count=None,
            less_than=None,
            check=True,
            schema=None,
            source=None,
            context=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
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

    @staticmethod
    def get_item_type():
        return sn.it.ItemType.SchemaRow

    def get_schema(self):
        return self.schema

    def set_schema(self, schema, check=True):
        return SchemaStream(
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
        return SchemaStream(
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

    def get_schema_rows(self):
        for r in self.get_items():
            yield sh.SchemaRow(r, self.get_schema(), check=False)

    def schematized_map(self, function, schema):
        return self.__class__(
            map(function, self.get_items()),
            count=self.count,
            less_than=self.count or self.less_than,
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
        expressions_list = [
            (k, fs.equal(v) if isinstance(v, (str, int, float, bool)) else v)
            for k, v in expressions.items()
        ]
        extended_filters_list = list(fields) + expressions_list

        def filter_function(r):
            for f in extended_filters_list:
                if not selection.value_from_schema_row(r, f):
                    return False
            return True
        props = self.get_meta()
        props.pop('count')
        filtered_items = filter(filter_function, self.get_items())
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            props['count'] = len(filtered_items)
        return self.__class__(
            filtered_items,
            **props
        )

