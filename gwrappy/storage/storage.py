from time import sleep
import random
from urllib2 import quote
from httplib2 import HttpLib2Error

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from gwrappy.service import get_service
from gwrappy.utils import iterate_list
from gwrappy.errors import HttpError
from gwrappy.storage.utils import GcsResponse


class GcsUtility:
    def __init__(self, **kwargs):
        self._service = get_service('storage', **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

        # Number of bytes to send/receive in each request.
        self._chunksize = kwargs.get('chunksize', 2 * 1024 * 1024)

        # Retry transport and file IO errors.
        self._RETRYABLE_ERRORS = (HttpLib2Error, IOError)

    def list_buckets(self, project_id, max_results=None, filter_exp=None):
        return iterate_list(
            self._service.buckets(),
            'items',
            max_results,
            self._max_retries,
            filter_exp,
            project=project_id
        )

    def list_objects(self, bucket_name, max_results=None, prefix=None, filter_exp=None):
        return iterate_list(
            self._service.objects(),
            'items',
            max_results,
            self._max_retries,
            filter_exp,
            bucket=bucket_name,
            prefix=prefix
        )

    @staticmethod
    def _parse_object_name(object_name):
        if isinstance(object_name, list):
            object_name = quote(('/'.join(object_name)))
        return object_name

    def get_object(self, bucket_name, object_name):
        resp = self._service.objects().get(
            bucket=bucket_name,
            object=self._parse_object_name(object_name)
        ).execute(num_retries=self._max_retries)

        return resp

    def delete_object(self, bucket_name, object_name):
        resp = self._service.objects().delete(
            bucket=bucket_name,
            object=self._parse_object_name(object_name)
        ).execute(num_retries=self._max_retries)

        if len(resp) > 0:
            raise AssertionError(resp)

    def _handle_progressless_iter(self, error, progressless_iters):
        if progressless_iters > self._max_retries:
            print 'Failed to make progress for too many consecutive iterations.'
            raise error

        sleep_time = random.random() * (2 ** progressless_iters)
        print ('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
               % (str(error), sleep_time, progressless_iters))
        sleep(sleep_time)

    def download_object(self, bucket_name, object_name, write_path):
        resp_obj = GcsResponse('downloaded')

        req = self._service.objects().get_media(
            bucket=bucket_name,
            object=self._parse_object_name(object_name)
        )

        write_file = file(write_path, 'wb')
        media = MediaIoBaseDownload(write_file, req, chunksize=self._chunksize)

        progressless_iters = 0
        done = False

        while not done:
            error = None
            try:
                progress, done = media.next_chunk()
            except HttpError as e:
                error = e
                if e.resp.status < 500:
                    raise
            except self._RETRYABLE_ERRORS as e:
                error = e

            if error:
                progressless_iters += 1
                self._handle_progressless_iter(error, progressless_iters)
            else:
                progressless_iters = 0

        resp_obj.load_resp(
            self.get_object(bucket_name, object_name),
            override_updated=True
        )
        return resp_obj

    def upload_object(self, bucket_name, object_name, read_path):
        resp_obj = GcsResponse('uploaded')

        media = MediaFileUpload(read_path, chunksize=self._chunksize, resumable=True)

        if not media.mimetype():
            media = MediaFileUpload(read_path, 'application/octet-stream', resumable=True)

        req = self._service.objects().insert(
            bucket=bucket_name,
            name=self._parse_object_name(object_name),
            media_body=media
        )

        progressless_iters = 0
        resp = None
        while resp is None:
            error = None
            try:
                progress, resp = req.next_chunk()
            except HttpError as e:
                error = e
                if e.resp.status < 500:
                    raise
            except self._RETRYABLE_ERRORS as e:
                error = e

            if error:
                progressless_iters += 1
                self._handle_progressless_iter(error, progressless_iters)
            else:
                progressless_iters = 0

        resp_obj.load_resp(
            resp
        )

        return resp_obj
