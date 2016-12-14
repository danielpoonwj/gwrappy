from googleapiclient.errors import Error


class JobError(Error):
    """Job status done but error present."""

    def __init__(self, resp):
        error_result = resp['status']['errorResult']

        self.id = resp['id']
        self.message = error_result.get('message', None)
        self.reason = error_result.get('reason', None)
        self.location = error_result.get('location', None)
        self.errors = resp['status'].get('errors', [])

    def __repr__(self):
        return '<JobError (%s) %s in %s: "%s">' % (
            self.id,
            self.reason,
            self.location,
            self.message
        )

    __str__ = __repr__
