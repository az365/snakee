from typing import Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_name
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.functions.arguments import get_name


class DetailedMessage:
    def __init__(self, **props):
        self.props = props
        self.key_fields = list()

    def copy(self):
        new_message = DetailedMessage(**self.props)
        new_message.set_key_fields(self.get_key_fields(), inplace=True)
        return new_message

    def get_fields(self):
        return self.props.keys()

    def set_key_fields(self, keys, inplace):
        if inplace:
            self.key_fields = keys
        else:
            new_message = self.copy()
            new_message.set_key_fields(keys, inplace=True)
            return new_message

    def get_key_fields(self):
        return tuple(self.key_fields)

    def get_key_values(self):
        return tuple([self.props.get(k) for k in self.get_key_fields()])

    def get_key_dict(self):
        return {k: self.props.get(k) for k in self.get_key_fields()}

    def get_key_str(self):
        return str(self.get_key_values())

    def get_detail_fields(self):
        return [f for f in self.get_fields() if f not in self.get_key_fields()]

    def get_detail_values(self):
        return [self.props.get(k) for k in self.get_detail_fields()]

    def get_details(self):
        return {f: self.props.get(f) for f in self.get_detail_fields()}

    def get_str(self):
        return str(self.props)


class SelectionError(DetailedMessage):
    def __init__(
            self,
            func: Union[Callable, str],
            in_fields: Iterable,
            in_values: Iterable,
            in_record: dict,
            message: str,
    ):
        func_name = get_name(func)
        if 'lambda' in func_name:
            func_name = 'lambda'
        super().__init__(
            func=func_name,
            in_fields=tuple(in_fields),
            in_values=tuple(in_values),
            in_record=tuple(in_record.items()),
            message=message,
        )
        self.set_key_fields(['func', 'in_fields', 'message'], inplace=True)

    def get_func_name(self):
        return self.props.get('func')

    def get_message(self):
        return self.props.get('message')

    def get_argument_fields(self):
        return self.props.get('in_fields')

    def get_argument_values(self):
        return self.props.get('in_values')

    def get_argument_fields_str(self):
        return ', '.join([str(a) for a in self.get_argument_fields()])

    def get_argument_list(self):
        return [(a, v) for a, v in zip(self.get_argument_fields(), self.get_argument_values())]

    def get_argument_str(self):
        return ', '.join(['{}={}'.format(a, v) for a, v in self.get_argument_list()])

    def get_key_str(self):
        return '{}({}) = {}'.format(self.get_func_name(), self.get_argument_fields_str(), self.get_message())

    def get_str(self):
        return '{}({}) = {}'.format(self.get_func_name(), self.get_argument_str(), self.get_message())
