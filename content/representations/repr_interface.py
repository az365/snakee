from abc import ABC, abstractmethod

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Optional, FieldID, Value
    from content.representations.repr_type import ReprType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import Optional, FieldID, Value
    from .repr_type import ReprType

OptKey = Optional[FieldID]


class RepresentationInterface(ABC):
    @staticmethod
    @abstractmethod
    def get_repr_type() -> ReprType:
        pass

    @abstractmethod
    def format(self, value: Value, skip_errors: bool = False) -> str:
        pass

    @abstractmethod
    def parse(self, line: str) -> Value:
        pass

    @abstractmethod
    def get_template(self, key: OptKey = None) -> str:
        pass

    @abstractmethod
    def get_default_template(self, key: OptKey = None) -> str:
        pass

    @abstractmethod
    def get_spec_str(self) -> str:
        pass

    @abstractmethod
    def get_default_spec_str(self) -> str:
        pass

    @abstractmethod
    def get_align_str(self) -> str:
        pass
