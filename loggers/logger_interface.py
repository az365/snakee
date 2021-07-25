from abc import ABC, abstractmethod
from typing import NoReturn


class LoggerInterface(ABC):
    @abstractmethod
    def log(self, msg: str, level: int, *args, **kwargs) -> NoReturn:
        pass

    @abstractmethod
    def debug(self, msg: str) -> NoReturn:
        pass

    @abstractmethod
    def info(self, msg: str) -> NoReturn:
        pass

    @abstractmethod
    def warning(self, msg: str) -> NoReturn:
        pass

    @abstractmethod
    def error(self, msg: str) -> NoReturn:
        pass

    @abstractmethod
    def critical(self, msg: str) -> NoReturn:
        pass
