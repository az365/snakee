from abc import ABC, abstractmethod
from typing import Type, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import AUTO, Auto

LoggingLevel = int
Native = Union[Type, Callable, int, Auto]


class LineOutputInterface(ABC):
    @classmethod
    @abstractmethod
    def get_output(cls, output: Native = AUTO) -> Native:
        pass

    @abstractmethod
    def output_line(self, line: str, output: Native = AUTO) -> None:
        pass

    @abstractmethod
    def output_blank_line(self, output: Native = AUTO) -> None:
        pass


AutoOutput = Union[Type, Callable, int, Auto]
