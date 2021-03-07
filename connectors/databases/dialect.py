DIALECTS = ('str', 'py', 'pg', 'ch')


def get_dialect_from_conn_type_name(name, default='py', other='str'):
    if name is None:
        return default
    elif 'Postgres' in name:
        return 'pg'
    elif 'Click' in name:
        return 'ch'
    else:
        return other


def get_dialect_for_connector(connector):
    type_name = None
    try:  # assume connector is connector class
        type_name = connector.__name__
    except AttributeError:
        pass
    if not type_name:
        try:  # assume connector is connector instance
            type_name = connector.__class__.__name__
        except AttributeError:
            pass
    if not type_name:
        try:  # assume connector is ConnType instance
            type_name = connector.value
        except AttributeError:
            pass
    return get_dialect_from_conn_type_name(type_name)
