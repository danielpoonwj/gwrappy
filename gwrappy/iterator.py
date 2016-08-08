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
        if filter_exp is not None:
            resp_list = [x for x in resp_list if filter_exp(x)]

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
            if filter_exp is not None:
                resp_list = [x for x in resp_list if filter_exp(x)]

            object_list += resp_list
            object_count = len(object_list)

        if max_results is not None and object_count > max_results:
            object_list = object_list[:max_results]
            break

    return object_list
