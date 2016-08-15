from googleapiclient.errors import Error


class JobError(Error):
    """Job status done but error present."""

    def __init__(self, resp):
        error_result = reduce(dict.__getitem__, ['status', 'errorResult'], resp)

        self.message = error_result.get('message', None)
        self.reason = error_result.get('reason', None)
        self.location = error_result.get('location', None)
        self.errors = resp['status'].get('errors', [])

    def __repr__(self):
        return '<JobError %s in %s: "%s">' % (
            self.reason, self.location, self.message
        )

    __str__ = __repr__
