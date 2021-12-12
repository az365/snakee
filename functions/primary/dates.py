from typing import Union, Optional
from datetime import date, timedelta, datetime

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.enum import DynamicEnum
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.enum import DynamicEnum

PyDate = date
IsoDate = str
GostDate = str
Date = Union[PyDate, IsoDate, GostDate]

DAYS_IN_YEAR = 365
MONTHS_IN_YEAR = 12
MEAN_DAYS_IN_MONTH = DAYS_IN_YEAR / MONTHS_IN_YEAR
MAX_DAYS_IN_MONTH = 31
DAYS_IN_WEEK = 7
WEEKS_IN_YEAR = 52

SECONDS_IN_MINUTE = 60
MINUTES_IN_HOUR = 60

DATE_SCALES = ('day', 'week', 'year')  # deprecated
ERR_TIME_SCALE = 'Expected time-scale {}, got {}'.format(' or '.join(DATE_SCALES), '{}')  # deprecated


class DateScale(DynamicEnum):
    Day = 'day'
    Week = 'week'
    Month = 'month'
    Year = 'year'

    @classmethod
    def get_err_msg(cls, scale='{}', available_scales=arg.AUTO):
        list_available_scales = arg.acquire(available_scales, cls._enum_items)
        if arg.is_defined(list_available_scales):
            str_available_scales = ' or '.join(list_available_scales)
        else:
            str_available_scales = '{}'
        return 'Expected time-scale {}, got {}'.format(str_available_scales, scale)

    @classmethod
    def convert(cls, scale, default=arg.AUTO, skip_missing=False):
        if isinstance(scale, str):
            if scale.startswith('d'):  # daily, day, days
                return DateScale.Day
            elif scale.startswith('w'):  # weekly, week, weeks
                return DateScale.Week
            elif scale.startswith('y'):  # yearly, year, years
                return DateScale.Year
            elif not skip_missing:
                # raise ValueError(ERR_TIME_SCALE.format(scale))
                raise ValueError(cls.get_err_msg(scale))
        return super().convert(scale, default=default, skip_missing=skip_missing)


DateScale.prepare()

_min_year = 2010


def get_min_year() -> int:
    return _min_year


def set_min_year(year: int):
    global _min_year
    _min_year = year


def is_py_date(d: Date) -> bool:
    return isinstance(d, PyDate)


def is_iso_date(d: Date) -> Optional[bool]:
    if isinstance(d, str):
        if len(d) >= 10:
            d = d[:10]
            return list(map(len, d.split('-'))) == [4, 2, 2]


def is_gost_date(d: Date) -> Optional[bool]:
    if isinstance(d, str):
        if len(d) >= 10:
            d = d[:10]
            return list(map(len, d.split('.'))) == [2, 2, 4]


def is_date(d: Date, also_gost_format: bool = False) -> bool:
    if is_py_date(d) or is_iso_date(d):
        return True
    elif also_gost_format:
        return is_gost_date(d)


def from_gost_format(d: GostDate, as_iso_date: bool = False) -> Date:
    gost_str = d.split(' ')[0]
    date_parts = gost_str.split('.')[:3]
    day, month, year = [int(i) for i in date_parts]
    if year < 100:
        year = 2000 + year
    standard_date = date(year=year, month=month, day=day)
    if as_iso_date:
        return standard_date.isoformat()
    else:
        return standard_date


def to_gost_format(d: Date) -> GostDate:
    d = to_date(d)
    return '{:02}.{:02}.{:04}'.format(d.day, d.month, d.year)


def raise_date_type_error(d):
    raise TypeError('Argument must be date in iso-format as str or python date (got {})'.format(type(d)))


def get_date(d: Date) -> PyDate:
    if isinstance(d, date):
        return d
    elif is_iso_date(d):
        return date.fromisoformat(d[:10])
    elif is_gost_date(d):
        return from_gost_format(d)
    else:
        raise_date_type_error(d)


def to_date(d: Date, as_iso_date: bool = False) -> Date:
    if as_iso_date:
        if is_iso_date(d):
            return d[:10]
        else:
            return d.isoformat()
    else:
        return get_date(d)


def get_shifted_date(d: Date, *args, **kwargs):
    as_iso_date = is_iso_date(d)
    shift = timedelta(*args, **kwargs)
    cur_date = get_date(d)
    shifted_date = cur_date + shift
    if as_iso_date:
        return to_date(shifted_date, as_iso_date=as_iso_date)
    else:
        return shifted_date


def get_month_from_date(d: Date) -> int:
    if is_iso_date(d):
        return date.fromisoformat(d).month
    elif isinstance(d, date):
        return d.month
    else:
        raise_date_type_error(d)


def get_month_first_date(d: Date) -> Date:
    if is_iso_date(d):
        return d[:8] + '01'
    elif isinstance(d, PyDate):
        return date(d.year, d.month, 1)
    else:
        raise_date_type_error(d)


def get_monday_date(d: Date, as_iso_date: Optional[bool] = None) -> Date:
    cur_date = get_date(d)
    if as_iso_date is None:
        as_iso_date = is_iso_date(d)
    monday_date = cur_date + timedelta(days=-cur_date.weekday())
    return to_date(monday_date, as_iso_date)


def get_year_first_date(d: [Date, int], as_iso_date: bool = True) -> Date:
    if isinstance(d, int):
        if d > 1900:
            return get_date_from_year(d, as_iso_date=as_iso_date)
        else:
            return get_date_from_year_and_month(year=d, month=1, as_iso_date=as_iso_date)
    else:  # isinstance(d, Date)
        year_no = get_year_from_date(d, decimal=False)
        iso_date = '{}-01-01'.format(year_no)
        if as_iso_date:
            return iso_date
        else:
            return get_date(iso_date)


def get_year_start_monday(year: int, as_iso_date: bool = True) -> Date:
    year_start_date = date(year, 1, 1)
    year_start_monday = year_start_date + timedelta(days=-year_start_date.weekday())
    return to_date(year_start_monday, as_iso_date)


def get_next_year_date(d: Date, step: int = 1, round_to_monday: bool = False) -> Date:
    is_iso_format = is_iso_date(d)
    if is_iso_format:
        dt = date.fromisoformat(d)
        dt = '{:04}-{:02}-{:02}'.format(dt.year + step, dt.month, dt.day)
    elif isinstance(d, date):
        dt = date(d.year + step, d.month, d.day)
    else:
        raise_date_type_error(d)
        dt = None
    if round_to_monday:
        return get_monday_date(dt, is_iso_format)
    else:
        return dt


def get_next_month_date(d: Date, step: int = 1, round_to_month: bool = False) -> Date:
    is_iso_format = is_iso_date(d)
    dt = to_date(d)
    month = dt.month
    year = dt.year
    month += step
    while month > MONTHS_IN_YEAR:
        month -= MONTHS_IN_YEAR
        year += 1
    while month < 0:
        month += MONTHS_IN_YEAR
        year -= 1
    dt = date(year=year, month=month, day=1 if round_to_month else dt.day)
    return to_date(dt, is_iso_format)


def get_next_week_date(d: Date, step: int = 1, round_to_monday: bool = False) -> Date:
    is_iso_format = is_iso_date(d)
    if is_iso_format:
        dt = date.fromisoformat(d)
    elif isinstance(d, date):
        dt = d
    else:
        raise_date_type_error(d)
        dt = None
    dt += timedelta(days=DAYS_IN_WEEK * step)
    if round_to_monday:
        dt = get_monday_date(d)
    if is_iso_format:
        return to_date(dt, is_iso_format)
    else:
        return dt


def get_next_day_date(d: Date, step: int = 1) -> Date:
    day_abs = get_day_abs_from_date(d)
    day_abs += step
    return get_date_from_day_abs(day_abs, as_iso_date=is_iso_date(d))


def get_days_range(
        date_min: Date, date_max: Date, step: int = 1,
        including_right: bool = True, as_day_abs: bool = False,
) -> list:
    days_range = list()
    cur_date = date_min
    while cur_date < date_max:
        days_range.append(cur_date)
        cur_date = get_next_day_date(cur_date, step=step)
    if including_right and cur_date == date_max:
        days_range.append(cur_date)
    if as_day_abs:
        return [get_day_abs_from_date(d) for d in days_range]
    else:
        return days_range


def get_weeks_range(
        date_min: Date, date_max: Date, step: int = 1,
        round_to_monday: bool = False, including_right: bool = True, as_week_abs: bool = False,
) -> list:
    weeks_range = list()
    cur_date = get_monday_date(date_min)
    if round_to_monday:
        date_min = cur_date
        date_max = get_monday_date(date_max)
    if cur_date < date_min:
        cur_date = get_next_week_date(cur_date, step=step)
    while cur_date < date_max:
        weeks_range.append(cur_date)
        cur_date = get_next_week_date(cur_date, step=step)
    if including_right and cur_date == date_max:
        weeks_range.append(cur_date)
    if as_week_abs:
        return [get_week_abs_from_date(d) for d in weeks_range]
    else:
        return weeks_range


def get_months_range(date_min: Date, date_max: Date, step: int = 1):
    months_range = list()
    cur_date = get_month_first_date(date_min)
    if cur_date < date_min:
        cur_date = get_next_month_date(cur_date, step=step)
    while cur_date <= date_max:
        months_range.append(cur_date)
        cur_date = get_next_month_date(cur_date, step=step)
    return months_range


def get_months_between(a: Date, b: Date, round_to_months: int = False, take_abs: bool = False) -> int:
    if round_to_months:
        a = get_month_first_date(a)
        b = get_month_first_date(b)
    days_between = get_days_between(a, b, take_abs=take_abs)
    months = int(days_between / int(MEAN_DAYS_IN_MONTH))
    return months


def get_weeks_between(a: Date, b: Date, round_to_mondays: bool = False, take_abs: bool = False) -> int:
    if round_to_mondays:
        a = get_monday_date(a, as_iso_date=False)
        b = get_monday_date(b, as_iso_date=False)
    days_between = get_days_between(a, b, take_abs=take_abs)
    weeks = int(days_between / DAYS_IN_WEEK)
    return weeks


def get_days_between(a: Date, b: Date, take_abs: bool = False) -> int:
    date_a = get_date(a)
    date_b = get_date(b)
    days = (date_b - date_a).days
    return abs(days) if take_abs else days


def get_yearly_dates(date_init: Date, date_min: Date, date_max: Date, step: int = 1) -> list:
    yearly_dates = list()
    cur_date = date_init
    while cur_date > date_min:
        cur_date = get_next_year_date(cur_date, step=-step)
    while cur_date <= date_max:
        if date_min <= cur_date <= date_max:
            yearly_dates.append(cur_date)
        cur_date = get_next_year_date(cur_date, step=step)
    return yearly_dates


def get_date_from_year_and_month(year: int, month: int, as_iso_date: bool = True) -> Date:
    iso_date = '{}-{:02}-01'.format(year, month)
    return to_date(iso_date, as_iso_date=as_iso_date)


def get_date_from_year_and_week(year: int, week: int, as_iso_date: bool = True) -> Date:
    year_start_monday = get_year_start_monday(year, as_iso_date=False)
    delta_days = week * DAYS_IN_WEEK
    cur_date = year_start_monday + timedelta(days=delta_days)
    return to_date(cur_date, as_iso_date)


def get_year_and_week_from_date(d: Date) -> tuple:
    cur_date = get_date(d)
    year = cur_date.year
    year_start_monday = get_year_start_monday(year, as_iso_date=False)
    days_since_year_start_monday = (cur_date - year_start_monday).days
    week = int(days_since_year_start_monday / DAYS_IN_WEEK)
    if week >= WEEKS_IN_YEAR:
        year += 1
        week = 0
    return year, week


def get_day_abs_from_date(d: Date, min_date: Union[Date, arg.Auto] = arg.AUTO) -> int:
    min_date = arg.delayed_acquire(min_date, get_year_start_monday, get_min_year())
    return get_days_between(min_date, d)


def get_month_abs_from_date(d: Date) -> int:
    month = get_month_from_date(d)
    delta_year = get_year_abs_from_date(d)
    return delta_year * MONTHS_IN_YEAR + month


def get_year_abs_from_date(d: Date) -> int:
    year = get_year_from_date(d)
    delta_year = year - get_min_year()
    return delta_year


def get_week_abs_from_year_and_week(
        year: int, week: int,
        min_year: Union[int, arg.Auto] = arg.AUTO,
) -> int:
    min_year = arg.acquire(min_year, get_min_year())
    week_abs = (year - min_year) * WEEKS_IN_YEAR + week
    return week_abs


def get_week_abs_from_date(
        d: Date,
        min_year: Union[int, arg.Auto] = arg.AUTO,
        decimal: bool = False,
) -> int:
    year, week = get_year_and_week_from_date(d)
    week_abs = get_week_abs_from_year_and_week(year, week, min_year=min_year)
    if decimal:
        week_abs += get_days_between(get_monday_date(d), d) / DAYS_IN_WEEK
    return week_abs


def get_week_no_from_date(d: Date) -> int:
    _, week_no = get_year_and_week_from_date(d)
    return week_no


def get_year_and_week_from_week_abs(week_abs: int, min_year: Union[int, arg.Auto] = arg.AUTO) -> tuple:
    min_year = arg.acquire(min_year, _min_year)
    delta_year = int(week_abs / WEEKS_IN_YEAR)
    year = min_year + delta_year
    week = week_abs - delta_year * WEEKS_IN_YEAR
    return year, week


def get_year_from_week_abs(week_abs: int, min_year: Union[int, arg.Auto] = arg.AUTO) -> int:
    min_year = arg.acquire(min_year, _min_year)
    delta_year = int(week_abs / WEEKS_IN_YEAR)
    return min_year + delta_year


def get_week_from_week_abs(week_abs: int) -> int:
    delta_year = int(week_abs / WEEKS_IN_YEAR)
    return week_abs - delta_year * WEEKS_IN_YEAR


def get_int_from_date(d: Date, scale: Union[DateScale, str]) -> int:
    scale = DateScale.convert(scale)
    if scale == DateScale.Day:
        return get_day_abs_from_date(d)
    elif scale == DateScale.Week:
        return get_week_abs_from_date(d)
    elif scale == DateScale.Month:
        return get_month_abs_from_date(d)
    elif scale == DateScale.Year:
        return get_year_abs_from_date(d)
    else:
        raise ValueError(DateScale.get_err_msg(scale))


def get_date_from_week_abs(
        week_abs: int,
        min_year: Union[int, arg.Auto] = arg.AUTO,
        as_iso_date: bool = True,
) -> Date:
    year, week = get_year_and_week_from_week_abs(week_abs, min_year=min_year)
    cur_date = get_date_from_year_and_week(year, week, as_iso_date=as_iso_date)
    return cur_date


def get_date_from_day_abs(
        day_abs: int,
        min_date: Union[Date, arg.Auto] = arg.AUTO,
        as_iso_date: bool = True,
) -> Date:
    min_date = arg.delayed_acquire(min_date, get_year_start_monday, get_min_year(), as_iso_date=as_iso_date)
    cur_date = get_shifted_date(min_date, days=day_abs)
    return cur_date


def get_date_from_month_abs(
        month_abs: int,
        min_date: Union[Date, arg.Auto] = arg.AUTO,
        as_iso_date: bool = True,
) -> Date:
    if arg.is_defined(min_date):
        min_year = get_year_from_date(min_date)
    else:
        min_year = get_min_year()
    year_delta = int((month_abs - 1) / MONTHS_IN_YEAR)
    year_no = min_year + year_delta
    month_no = month_abs - year_delta * MONTHS_IN_YEAR
    cur_date = get_date_from_year_and_month(year=year_no, month=month_no, as_iso_date=as_iso_date)
    return cur_date


def get_date_from_year(year: int, as_iso_date: bool = True) -> Date:
    int_year = int(year)
    year_part = year - int_year
    cur_date = get_year_start_monday(year=int_year, as_iso_date=as_iso_date)
    if year_part:
        cur_date = get_shifted_date(cur_date, days=year_part * DAYS_IN_YEAR)
    return cur_date


def get_year_from_date(d: Date, decimal: bool = False) -> Union[int, float]:
    year = get_date(d).year
    if decimal:
        year += get_days_between(get_year_start_monday(year), d) / DAYS_IN_YEAR
    return year


def get_year_decimal_from_date(d: Date) -> float:
    return get_year_from_date(d, decimal=True)


def get_date_from_numeric(numeric: int, from_scale: Union[DateScale, str] = DateScale.Day) -> Date:
    scale = DateScale.convert(from_scale)
    if scale == DateScale.Day:
        func = get_date_from_day_abs
    elif scale == DateScale.Week:
        func = get_date_from_week_abs
    elif scale == DateScale.Month:
        func = get_date_from_month_abs
    elif scale == DateScale.Year:
        func = get_date_from_year
    else:
        raise ValueError(DateScale.get_err_msg(scale))
    return func(numeric)


def get_days_in_scale(scale: Union[DateScale, str]) -> int:
    scale = DateScale.convert(scale)
    if scale == DateScale.Day:
        return 1
    elif scale == DateScale.Week:
        return DAYS_IN_WEEK
    elif scale == DateScale.Month:
        return MAX_DAYS_IN_MONTH
    elif scale == DateScale.Year:
        return DAYS_IN_YEAR
    else:
        raise ValueError(DateScale.get_err_msg(scale))


def get_formatted_datetime(dt: datetime) -> str:
    return dt.isoformat()[:16].replace('T', ' ')


def get_current_time_str():
    return get_formatted_datetime(datetime.now())


def get_str_from_timedelta(td: timedelta) -> str:
    td_abs = abs(td)
    if td_abs.days >= DAYS_IN_YEAR:
        td_str = '{}Y'.format(int(td_abs.days / DAYS_IN_YEAR))
    elif td_abs.days >= MEAN_DAYS_IN_MONTH:
        td_str = '{}M'.format(int(td_abs.days / MEAN_DAYS_IN_MONTH))
    elif td_abs.days >= DAYS_IN_WEEK:
        td_str = '{}W'.format(int(td_abs.days / DAYS_IN_WEEK))
    elif td_abs.days >= 1:
        td_str = '{}D'.format(int(td_abs.days))
    elif td_abs.seconds >= SECONDS_IN_MINUTE * MINUTES_IN_HOUR:
        td_str = '{}h'.format(int(td_abs.seconds / SECONDS_IN_MINUTE / MINUTES_IN_HOUR))
    elif td_abs.seconds >= SECONDS_IN_MINUTE:
        td_str = '{}m'.format(int(td_abs.seconds / SECONDS_IN_MINUTE))
    elif td_abs.seconds:
        td_str = '{}s'.format(td_abs.seconds)
    else:
        td_str = '0'
    if td.seconds >= 0:
        return td_str
    else:
        return '-{}'.format(td_str)
