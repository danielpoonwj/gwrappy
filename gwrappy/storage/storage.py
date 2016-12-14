from time import sleep
import random

# python 2/3 compatibility
try:
    from urllib2 import quote
except ImportError:
    from urllib.request import quote

from httplib2 import HttpLib2Error

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from gwrappy.service import get_service
from gwrappy.utils import iterate_list
from gwrappy.errors import HttpError
from gwrappy.storage.utils import GcsResponse


class GcsUtility:
    def __init__(self, **kwargs):
        """
        Initializes object for interacting with Google Cloud Storage API.

        |  By default, Application Default Credentials are used.
        |  If gcloud SDK isn't installed, credential files have to be specified using the kwargs *json_credentials_path* and *client_id*.

        :keyword max_retries: Argument specified with each API call to natively handle retryable errors.
        :type max_retries: integer
        :keyword chunksize: Upload/Download chunk size
        :type chunksize: integer
        :keyword client_secret_path: File path for client secret JSON file. Only required if credentials are invalid or unavailable.
        :keyword json_credentials_path: File path for automatically generated credentials.
        :keyword client_id: Credentials are stored as a key-value pair per client_id to facilitate multiple clients using the same credentials file. For simplicity, using one's email address is sufficient.
        """

        self._service = get_service('storage', **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

        # Number of bytes to send/receive in each request.
        self._chunksize = kwargs.get('chunksize', 2 * 1024 * 1024)

        # Retry transport and file IO errors.
        self._RETRYABLE_ERRORS = (HttpLib2Error, IOError)

    def list_buckets(self, project_id, max_results=None, filter_exp=None):
        """
        Abstraction of buckets().list() method with inbuilt iteration functionality. [https://cloud.google.com/storage/docs/json_api/v1/buckets/list]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_exp: Function that filters entries if filter_exp evaluates to True.
        :type filter_exp: function
        :return: List of dictionary objects representing bucket resources.
        """

        return iterate_list(
            self._service.buckets(),
            'items',
            max_results,
            self._max_retries,
            filter_exp,
            project=project_id
        )

    def list_objects(self, bucket_name, max_results=None, prefix=None, projection=None, filter_exp=None):
        """
        Abstraction of objects().list() method with inbuilt iteration functionality. [https://cloud.google.com/storage/docs/json_api/v1/objects/list]

        :param bucket_name: Bucket identifier.
        :type bucket_name: string
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param prefix: Pre-filter (on API call) results to objects whose names begin with this prefix.
        :type prefix: string
        :param projection: Set of properties to return.
        :param filter_exp: Function that filters entries if filter_exp evaluates to True.
        :type filter_exp: function
        :return: List of dictionary objects representing object resources.
        """

        return iterate_list(
            self._service.objects(),
            'items',
            max_results,
            self._max_retries,
            filter_exp,
            bucket=bucket_name,
            prefix=prefix,
            projection=projection
        )

    @staticmethod
    def _parse_object_name(object_name):
        if isinstance(object_name, list):
            object_name = quote(('/'.join(object_name)))
        return object_name

    def get_object(self, bucket_name, object_name, projection=None):
        """
        Abstraction of objects().get() method with inbuilt iteration functionality. [https://cloud.google.com/storage/docs/json_api/v1/objects/get]

        :param bucket_name: Bucket identifier.
        :type bucket_name: string
        :param object_name: Can take string representation of object resource or list denoting path to object on GCS.
        :type object_name: list or string
        :param projection: Set of properties to return.
        :return: Dictionary object representing object resource.
        """

        resp = self._service.objects().get(
            bucket=bucket_name,
            object=self._parse_object_name(object_name),
            projection=projection
        ).execute(num_retries=self._max_retries)

        return resp

    def update_object(self, bucket_name, object_name, predefined_acl=None, projection=None, **object_resource):
        """
        Abstraction of objects().update() method. [https://cloud.google.com/storage/docs/json_api/v1/objects/update]

        :param bucket_name: Bucket identifier.
        :type bucket_name: string
        :param object_name: Can take string representation of object resource or list denoting path to object on GCS.
        :type object_name: list or string
        :param predefined_acl: Apply a predefined set of access controls to this object.
        :param projection: Set of properties to return.
        :param object_resource: Supply optional properties [https://cloud.google.com/storage/docs/json_api/v1/objects/insert#request-body]
        :return: Dictionary object representing object resource.
        """

        resp = self._service.objects().update(
            bucket=bucket_name,
            object=self._parse_object_name(object_name),

            predefinedAcl=predefined_acl,
            projection=projection,
            body=object_resource
        ).execute(num_retries=self._max_retries)

        return resp

    def delete_object(self, bucket_name, object_name):
        """
        Abstraction of objects().delete() method with inbuilt iteration functionality. [https://cloud.google.com/storage/docs/json_api/v1/objects/delete]

        :param bucket_name: Bucket identifier.
        :type bucket_name: string
        :param object_name: Can take string representation of object resource or list denoting path to object on GCS.
        :type object_name: list or string
        :raises: AssertionError if unsuccessful. Response should be empty string if successful.
        """

        resp = self._service.objects().delete(
            bucket=bucket_name,
            object=self._parse_object_name(object_name)
        ).execute(num_retries=self._max_retries)

        if len(resp) > 0:
            raise AssertionError(resp)

    def _handle_progressless_iter(self, error, progressless_iters):
        if progressless_iters > self._max_retries:
            print('Failed to make progress for too many consecutive iterations.')
            raise error

        sleep_time = random.random() * (2 ** progressless_iters)
        print('Caught exception (%s). Sleeping for %s seconds before retry #%d.' %
              (str(error), sleep_time, progressless_iters))
        sleep(sleep_time)

    def download_object(self, bucket_name, object_name, write_path):
        """
        Downloads object in chunks.

        :param bucket_name: Bucket identifier.
        :type bucket_name: string
        :param object_name: Can take string representation of object resource or list denoting path to object on GCS.
        :type object_name: list or string
        :param write_path: Local path to write object to.
        :type write_path: string
        :returns: GcsResponse object.
        :raises: HttpError if non-retryable errors are encountered.
        """

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
            is_download=True
        )
        return resp_obj

    def upload_object(self, bucket_name, object_name, read_path, predefined_acl=None, projection=None, **object_resource):
        """
        Uploads object in chunks.

        Optional parameters and valid object resources are listed here [https://cloud.google.com/storage/docs/json_api/v1/objects/insert]

        :param bucket_name: Bucket identifier.
        :type bucket_name: string
        :param object_name: Can take string representation of object resource or list denoting path to object on GCS.
        :type object_name: list or string
        :param read_path: Local path of object to upload.
        :type read_path: string
        :param predefined_acl: Apply a predefined set of access controls to this object.
        :param projection: Set of properties to return.
        :param object_resource: Supply optional properties [https://cloud.google.com/storage/docs/json_api/v1/objects/insert#request-body]
        :returns: GcsResponse object.
        :raises: HttpError if non-retryable errors are encountered.
        """
        resp_obj = GcsResponse('uploaded')

        media = MediaFileUpload(read_path, chunksize=self._chunksize, resumable=True)

        if not media.mimetype():
            media = MediaFileUpload(read_path, 'application/octet-stream', resumable=True)

        req = self._service.objects().insert(
            bucket=bucket_name,
            name=self._parse_object_name(object_name),
            media_body=media,

            predefinedAcl=predefined_acl,
            projection=projection,
            body=object_resource
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
            resp,
            is_download=False
        )

        return resp_obj
