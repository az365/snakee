from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg, dates as dt
    from interfaces import LeafConnectorInterface, AUTO, Auto, AutoCount, AutoBool
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg, dates as dt
    from ...interfaces import LeafConnectorInterface, AUTO, Auto, AutoCount, AutoBool

Native = LeafConnectorInterface


class AppropriateInterface(LeafConnectorInterface, ABC):
    @abstractmethod
    def get_modification_timestamp(self, reset: bool = True) -> float:
        pass

    @abstractmethod
    def get_prev_modification_timestamp(self) -> Optional[float]:
        pass

    @abstractmethod
    def set_prev_modification_timestamp(self, timestamp: float):
        pass

    @abstractmethod
    def get_expected_count(self):
        pass

    @abstractmethod
    def set_count(self, count: int):
        pass

    @abstractmethod
    def is_gzip(self) -> bool:
        pass

    @abstractmethod
    def is_verbose(self) -> bool:
        pass

    @abstractmethod
    def is_opened(self) -> bool:
        pass

    @abstractmethod
    def is_closed(self) -> bool:
        pass

    @abstractmethod
    def open(self, allow_reopen: bool):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False, allow_reopen: bool = True,
            check: bool = True, verbose: AutoBool = AUTO,
            message: Union[str, Auto] = AUTO, step: AutoCount = AUTO,
    ) -> Iterable:
        pass

    @abstractmethod
    def get_chunks(self):
        pass


class ActualizeMixin(AppropriateInterface, ABC):
    def is_changed_by_another(self) -> bool:
        return not self.is_actual()

    def is_actual(self) -> bool:
        return self.get_modification_timestamp() == self.get_prev_modification_timestamp()

    def actualize(self) -> Native:
        self.get_modification_timestamp()
        self.get_count(force=True)
        return self

    def get_modification_time_str(self) -> str:
        timestamp = dt.datetime.fromtimestamp(self.get_modification_timestamp())
        return dt.get_formatted_datetime(timestamp)

    def reset_modification_timestamp(self, timestamp: Union[float, Auto, None] = AUTO) -> Native:
        timestamp = arg.acquire(timestamp, self.get_modification_timestamp(reset=False))
        return self.set_prev_modification_timestamp(timestamp)

    def get_file_age_str(self):
        timestamp = self.get_modification_timestamp()
        if timestamp:
            timedelta_age = dt.datetime.now() - dt.datetime.fromtimestamp(timestamp)
            assert isinstance(timedelta_age, dt.timedelta)
            if timedelta_age.seconds == 0:
                return 'now'
            elif timedelta_age.seconds > 0:
                return dt.get_str_from_timedelta(timedelta_age)
            else:
                return 'future'

    def get_datetime_str(self) -> str:
        if self.is_existing():
            times = self.get_modification_time_str(), self.get_file_age_str(), dt.get_current_time_str()
            return '{} + {} = {}'.format(*times)
        else:
            return dt.get_current_time_str()

    def get_prev_lines_count(self) -> Optional[AutoCount]:
        return self.get_expected_count()

    def get_slow_lines_count(self, verbose: AutoBool = AUTO) -> int:
        count = 0
        for _ in self.get_lines(message='Slow counting lines in {}...', allow_reopen=True, verbose=verbose):
            count += 1
        self.set_count(count)
        return count

    def get_fast_lines_count(self, ending: Union[str, Auto] = AUTO, verbose: AutoBool = AUTO) -> int:
        if self.is_gzip():
            raise ValueError('get_fast_lines_count() method is not available for gzip-files')
        if not arg.is_defined(ending):
            if hasattr(self, 'get_content_format'):
                ending = self.get_content_format().get_ending()
            else:
                ending = '\n'
        verbose = arg.acquire(verbose, self.is_verbose())
        self.log('Counting lines in {}...'.format(self.get_name()), end='\r', verbose=verbose)
        count_n_symbol = sum(chunk.count(ending) for chunk in self.get_chunks())
        count_lines = count_n_symbol + 1
        self.set_count(count_lines)
        return count_lines

    def get_actual_lines_count(self, allow_reopen: bool = True, allow_slow_gzip: bool = True) -> Optional[int]:
        if self.is_opened():
            if allow_reopen:
                self.close()
            else:
                raise ValueError('File is already opened: {}'.format(self))
        self.open(allow_reopen=allow_reopen)
        if self.is_gzip():
            if allow_slow_gzip:
                count = self.get_slow_lines_count()
            else:
                count = None
        else:
            count = self.get_fast_lines_count()
        self.close()
        if count is not None:
            self.log('Detected {} lines in {}.'.format(count, self.get_name()), end='\r')
        return count

    def get_count(self, allow_reopen: bool = True, allow_slow_gzip: bool = True, force: bool = False) -> Optional[int]:
        must_recount = force or self.is_changed_by_another() or not arg.is_defined(self.get_prev_lines_count())
        if self.is_existing() and must_recount:
            count = self.get_actual_lines_count(allow_reopen=allow_reopen, allow_slow_gzip=allow_slow_gzip)
            self.set_count(count)
        else:
            count = self.get_prev_lines_count()
        if arg.is_defined(count):
            return count
