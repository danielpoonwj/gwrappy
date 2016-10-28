from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pytz import timezone
from tzlocal import get_localzone

import logging

# python 2/3 compatibility
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


def iterate_list(service, object_name, max_results=None, max_retries=3, filter_exp=None, break_condition=None, **kwargs):
    object_count = 0

    resp = service.list(
        **kwargs if kwargs is not None else {}
    ).execute(num_retries=max_retries)

    if object_name in resp:
        for x in resp[object_name]:
            if filter_exp is not None and not filter_exp(x):
                continue

            # if max_results is None, get all results
            if max_results is not None and object_count >= max_results:
                resp.pop('nextPageToken', None)
                break

            # break condition mainly used to limit jobs dates which are sorted in reverse chronological order
            if break_condition is not None and break_condition(x):
                resp.pop('nextPageToken', None)
                break

            object_count += 1
            yield x

    while 'nextPageToken' in resp:
        page_token = resp.get('nextPageToken', None)

        resp = service.list(
            pageToken=page_token,
            **kwargs if kwargs is not None else {}
        ).execute(num_retries=max_retries)

        if object_name in resp:
            for x in resp[object_name]:
                if filter_exp is not None and not filter_exp(x):
                    continue

                if max_results is not None and object_count >= max_results:
                    resp.pop('nextPageToken', None)
                    break

                if break_condition is not None and break_condition(x):
                    resp.pop('nextPageToken', None)
                    break

                object_count += 1
                yield x


def timestamp_to_datetime(input_timestamp, tz=None):
    """
    Converts epoch timestamp into datetime object.

    :param input_timestamp: Epoch timestamp. Microsecond or millisecond inputs accepted.
    :type input_timestamp: long
    :param tz: String representation of timezone accepted by pytz. eg. 'Asia/Hong_Kong'. If param is unfilled, system timezone is used.
    :return: timezone aware datetime object
    """
    input_timestamp = long(input_timestamp)

    if tz is None:
        tz = get_localzone()
    else:
        tz = timezone(tz)

    # if timestamp granularity is microseconds
    try:
        return_value = datetime.fromtimestamp(input_timestamp, tz=tz)
    except ValueError:
        input_timestamp = float(input_timestamp)/1000
        return_value = datetime.fromtimestamp(input_timestamp, tz=tz)

    return return_value


def datetime_to_timestamp(input_datetime, date_format='%Y-%m-%d %H:%M:%S', tz=None):
    """
    Converts datetime to epoch timestamp.

    **Note** - If input_datetime is timestamp aware, it would first be localized according to the tz parameter if filled, or the system timezone if unfilled.

    :param input_datetime: Date to convert.
    :type input_datetime: datetime object or string representation of datetime.
    :param date_format: If input is string, denotes string datetime format to convert from.
    :param tz: String representation of timezone accepted by pytz. eg. 'Asia/Hong_Kong'. If param is unfilled, system timezone is used.
    :return: timezone aware datetime object
    """
    epoch = timezone('UTC').localize(datetime.utcfromtimestamp(0))

    if isinstance(input_datetime, str):
        input_datetime = datetime.strptime(input_datetime, date_format)

    assert isinstance(input_datetime, datetime)

    if tz is None:
        tz = get_localzone()
    else:
        tz = timezone(tz)

    if input_datetime.tzinfo is None:
        input_value = tz.localize(input_datetime)
    else:
        input_value = input_datetime.astimezone(tz)

    return_value = long((input_value - epoch).total_seconds())

    return return_value


def date_range(start, end, ascending=True, date_format='%Y-%m-%d'):
    """
    Simple datetime generator for dates between start and end (inclusive).

    :param start: Date to start at.
    :type start: datetime object or string representation of datetime.
    :param end: Date to stop at.
    :type end: datetime object or string representation of datetime.
    :param ascending: Toggle sorting of output.
    :type ascending: boolean
    :param date_format: If input is string, denotes string datetime format to convert from.
    :return: generator object for naive datetime objects
    """
    if isinstance(start, str):
        start_date = datetime.strptime(start, date_format)
    else:
        start_date = start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    if isinstance(end, str):
        end_date = datetime.strptime(end, date_format)
    else:
        end_date = end.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    assert end_date >= start_date

    days_apart = (end_date - start_date).days + 1

    for i in (range(0, days_apart) if ascending else range(0, days_apart)[::-1]):
        yield start_date + timedelta(i)


def month_range(start, end, full_months=False, month_format='%Y-%m', ascending=True, date_format='%Y-%m-%d'):
    """
    Simple utility to chunk date range into months.

    :param start: Date to start at.
    :param end: Date to end at.
    :param full_months: If true, only data up till the last complete month would be included.
    :param month_format: Format for month key.
    :param date_format: If start and end is string, denotes string datetime format to convert from.
    :param ascending: Sort date list ascending
    :return: dictionary keyed by month (in the format specified by month_format) with values being the list of dates within that month.
    """

    def is_last_day(input_date):
        return input_date == (input_date.replace(day=1) + relativedelta(months=1) - timedelta(days=1))

    date_dict = {}
    for temp_date in date_range(start, end, date_format=date_format):
        month_key = temp_date.strftime(month_format)
        date_dict.setdefault(month_key, [])
        date_dict[month_key].append(temp_date)

    if full_months:
        return {k: sorted(v, reverse=not ascending) for k, v in date_dict.items() if is_last_day(max(v))}
    else:
        return {k: sorted(v, reverse=not ascending) for k, v in date_dict.items()}


def simple_mail(send_to, subject, text, send_from=None, username=None, password=None, server='smtp.gmail.com', port=587):
    """
    Simple utility mail function - only text messages without attachments.

    *Note* - In Gmail you'd have to allow 'less secure apps to access your account'. Not recommended for secure information/accounts.

    :param send_to: Email recipients
    :type send_to: list or string
    :param subject: Email Subject
    :param text: Email Body
    :param send_from: Name of sender
    :param username: Login username
    :param password: Login password
    :param server: Mail server
    :param port: Connection port
    """

    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.utils import COMMASPACE, formatdate

    assert username is not None and password is not None

    if send_from is None:
        send_from = username

    if not isinstance(send_to, list):
        send_to = [send_to]

    message = MIMEMultipart()
    message['From'] = send_from
    message['To'] = COMMASPACE.join(send_to)
    message['Date'] = formatdate(localtime=True)
    message['Subject'] = subject

    message.attach(MIMEText(text))

    smtp = smtplib.SMTP(server, port)
    smtp.starttls()

    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg=message.as_string())
    smtp.quit()


class StringLogger:
    def __init__(self, name=None, level=logging.INFO, formatter=None, ignore_modules=None):
        """
        Simple logging wrapper with a string buffer handler to easily write and retrieve logs as strings.

        :param name: Name of logger
        :param level: Logging level
        :param formatter: logging.Formatter() object
        :param ignore_modules: list of module names to ignore from logging process
        """

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if formatter is None:
            self._formatter = logging.Formatter(
                    fmt='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            assert isinstance(formatter, logging.Formatter)
            self._formatter = formatter

        self._log_capture_string = StringIO()
        sh = logging.StreamHandler(self._log_capture_string)
        sh.setLevel(level)
        sh.setFormatter(self._formatter)

        self.logger.addHandler(sh)

        # filters logging for modules in ignore_modules
        if ignore_modules is not None and isinstance(ignore_modules, (list, tuple)):
            class _LoggingFilter(logging.Filter):
                def filter(self, record):
                    # still lets WARNING, ERROR and CRITICAL through
                    return record.name not in ignore_modules or \
                           record.levelno in (logging.WARNING, logging.ERROR, logging.CRITICAL)

            for handler in self.logger.handlers:
                handler.addFilter(_LoggingFilter())

    def get_logger(self):
        """
        Return instantiated Logger.

        :return: logging.Logger object
        """

        return self.logger

    def get_log_string(self):
        """
        Return logs as string.

        :return: logged data as string
        """

        return self._log_capture_string.getvalue()

    def close(self):
        self._log_capture_string.close()
