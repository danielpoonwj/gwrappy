from datetime import datetime, timedelta
from pytz import timezone
from tzlocal import get_localzone


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
