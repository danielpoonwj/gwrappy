from datetime import datetime
from pytz import UTC
import humanize


class GcsResponse:
    def __init__(self, description):
        """
        Wrapper for GCS upload and download responses, mainly for calculating/parsing job statistics into human readable formats for logging.

        :param description: String descriptor for specific function of job.
        """
        self.description = description.strip().title()
        self.start()

    def start(self):
        setattr(self, 'start_time', datetime.now(UTC))

    def load_resp(self, resp, is_download):
        """
        Loads json response from API.

        :param resp: Response from API
        :type resp: dictionary
        :param is_download: Calculates time taken based on 'updated' field in response if upload, and based on stop time if download
        :type is_download: boolean
        """

        assert isinstance(resp, dict)
        setattr(self, 'resp', resp)
        setattr(self, 'size', humanize.naturalsize(int(resp['size'])))

        if is_download:
            updated_at = datetime.now(UTC)
        else:
            updated_at = UTC.localize(datetime.strptime(resp['updated'], '%Y-%m-%dT%H:%M:%S.%fZ'))

        setattr(self, 'time_taken', dict(zip(
            ('m', 's'),
            divmod((updated_at - getattr(self, 'start_time')).seconds if updated_at > getattr(self, 'start_time') else 0, 60)
        )))

        setattr(self, 'full_path', 'gs://%s/%s' % (resp['bucket'], resp['name']))

    def __repr__(self):
        return '[GCS] %s %s %s(%s)' % (
            self.description,
            getattr(self, 'full_path'),
            '%s ' % getattr(self, 'size') if hasattr(self, 'size') else '',
            '{m} Minutes {s} Seconds'.format(**getattr(self, 'time_taken'))
        )

    __str__ = __repr__
