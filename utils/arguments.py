DEFAULT_VALUE = 'DefaultArgument'


class DefaultArgument:
    @staticmethod
    def get_value():
        return DEFAULT_VALUE

    def __str__(self):
        return str(self.get_value())

    def __eq__(self, other):
        return other == self.get_value()


DEFAULT = DefaultArgument()


def update(args, addition=None):
    if addition:
        args = list(args) + (addition if isinstance(addition, (list, tuple)) else [addition])
    if len(args) == 1 and isinstance(args[0], (list, tuple)):
        args = args[0]
    return args


def undefault(current, default):
    if current == DEFAULT:
        return default
    else:
        return current
