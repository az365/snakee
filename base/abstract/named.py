from abc import ABC

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract.abstract_base import AbstractBaseObject
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .abstract_base import AbstractBaseObject

SPECIFIC_MEMBERS = ('_name', )


class AbstractNamed(AbstractBaseObject, ABC):
    def __init__(self, name: str):
        super().__init__()
        self._name = name

    def get_name(self) -> str:
        return self._name

    def set_name(self, name: str, inplace=True):
        if inplace:
            self._name = name
        else:
            return self.__class__(
                name=name,
                **self.get_props(ex='name')
            )

    @classmethod
    def _get_key_member_names(cls) -> list:
        return super()._get_key_member_names() + list(SPECIFIC_MEMBERS)

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return cls._get_key_member_names()

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self.get_name())
