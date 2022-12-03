import datetime
import re

DATE_RE = re.compile(r"^(\d\d\d\d)-0?([1-9]?\d)-0?([1-9]?\d)")


def parse(string_date):
    """
    :param string_date: '2019-03-05'
    :return: datetime.date(2019, 3, 5)
    """
    # This would be less code, but it is around 4.5x slower:
    # return datetime.datetime.strptime(string_date, '%Y-%m-%d').date()

    # Faster version:
    match = DATE_RE.match(string_date)
    return datetime.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))


def parse_relative_date(date_str, relative_to):
    if date_str is None:
        return 0
    if isinstance(relative_to, datetime.datetime):
        relative_to = relative_to.date()
    delta = parse(date_str) - relative_to
    seconds_per_year = 86400 * 365
    return delta.total_seconds() / seconds_per_year
