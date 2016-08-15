from datetime import datetime
from pytz import timezone
from tzlocal import get_localzone


def iterate_list(service, object_name, max_results=None, max_retries=3, filter_exp=None, **kwargs):
    object_list = []
    object_count = 0

    if kwargs is None:
        kwargs = {}

    resp = service.list(
        **kwargs
    ).execute(num_retries=max_retries)

    if object_name in resp:
        resp_list = resp[object_name]
        resp_list = filter(filter_exp, resp_list)

        object_list += resp_list
        object_count = len(object_list)
    else:
        return []

    if max_results is not None and object_count > max_results:
        object_list = object_list[:max_results]
        return object_list

    while 'nextPageToken' in resp:
        page_token = resp.get('nextPageToken', None)

        resp = service.list(
            pageToken=page_token,
            **kwargs
        ).execute(num_retries=max_retries)

        if object_name in resp:
            resp_list = resp[object_name]
            resp_list = filter(filter_exp, resp_list)

            object_list += resp_list
            object_count = len(object_list)

        if max_results is not None and object_count > max_results:
            object_list = object_list[:max_results]
            break

    return object_list


def timestamp_to_datetime(input_timestamp, tz=None):
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


def datetime_to_timestamp(input_datetime, date_format='%Y-%m-%d', tz=None):
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
