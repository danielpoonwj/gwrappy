from datetime import datetime


class OperationResponse:
    def __init__(self, resp):
        """
        Wrapper for Dataproc Operation responses, mainly for calculating/parsing statistics into human readable formats for logging.

        :param resp: Dictionary representation of an operation resource.
        :type resp: dictionary
        """

        self.resp = resp
        self.name = resp['name']
        self.type = resp['metadata']['operationType']

        self._parse_timing()

    def _parse_timing(self):
        try:
            start_time = datetime.strptime(
                self.resp['metadata']['statusHistory'][0]['stateStartTime'],
                '%Y-%m-%dT%H:%M:%S.%fZ'
            )

            end_time = datetime.strptime(
                self.resp['metadata']['status']['stateStartTime'],
                '%Y-%m-%dT%H:%M:%S.%fZ'
            )

            time_taken = (end_time - start_time).total_seconds()

            setattr(
                self,
                'time_taken',
                dict(zip(
                    ('m', 's'),
                    divmod(time_taken, 60)
                ))
            )
        except (IndexError, KeyError):
            pass

    def __repr__(self):
        return '[Dataproc] %s Operation (%s)%s' % (
            self.type,
            self.name.split('/')[-1],
            ' ({m:.0f} Minutes {s:.0f} Seconds)'.format(**getattr(self, 'time_taken')) if hasattr(self, 'time_taken') else ''
        )

    __str__ = __repr__


class JobResponse:
    def __init__(self, resp):
        """
        Wrapper for Dataproc Job responses, mainly for calculating/parsing statistics into human readable formats for logging.

        :param resp: Dictionary representation of an operation resource.
        :type resp: dictionary
        """

        self.resp = resp
        self.id = '{projectId}:{jobId}'.format(**resp['reference'])
        self.state = resp['status']['state']

        self.job_type = set(tuple(resp.keys()))\
            .intersection({'hadoopJob', 'sparkJob', 'pysparkJob', 'hiveJob', 'pigJob', 'sparkSqlJob'})\
            .pop()

        self._parse_timing()

    def _parse_timing(self):
        try:
            start_time = datetime.strptime(
                self.resp['statusHistory'][0]['stateStartTime'],
                '%Y-%m-%dT%H:%M:%S.%fZ'
            )

            end_time = datetime.strptime(
                self.resp['status']['stateStartTime'],
                '%Y-%m-%dT%H:%M:%S.%fZ'
            )

            time_taken = (end_time - start_time).total_seconds()

            setattr(
                self,
                'time_taken',
                dict(zip(
                    ('m', 's'),
                    divmod(time_taken, 60)
                ))
            )
        except (IndexError, KeyError):
            pass

    def __repr__(self):
        return '[Dataproc] %s (%s) %s%s' % (
            self.job_type[0].upper() + self.job_type[1:],
            self.id,
            self.state,
            ' ({m:.0f} Minutes {s:.0f} Seconds)'.format(**getattr(self, 'time_taken')) if hasattr(self, 'time_taken') else ''
        )

    __str__ = __repr__
