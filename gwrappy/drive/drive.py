import os
import io

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from gwrappy.service import get_service
from gwrappy.utils import iterate_list
from gwrappy.errors import HttpError
from gwrappy.drive.utils import DriveResponse


class DriveUtility:
    def __init__(self, json_credentials_path, client_id, **kwargs):
        """
        Initializes object for interacting with Bigquery API.

        :param client_secret_path: File path for client secret JSON file. Only required if credentials are invalid or unavailable.
        :param json_credentials_path: File path for automatically generated credentials.
        :param client_id: Credentials are stored as a key-value pair per client_id to facilitate multiple clients using the same credentials file. For simplicity, using one's email address is sufficient.
        :keyword max_retries: Argument specified with each API call to natively handle retryable errors.
        :type max_retries: integer
        :keyword chunksize: Upload/Download chunk size
        :type chunksize: integer
        """
        self._service = get_service('drive', json_credentials_path=json_credentials_path, client_id=client_id, **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

        # Number of bytes to send/receive in each request.
        self._chunksize = kwargs.get('chunksize', 2 * 1024 * 1024)

    def get_account_info(self, fields=None):
        """
        Abstraction of about().get() method. [https://developers.google.com/drive/v3/reference/about/get]

        :param fields: Available properties can be found here: https://developers.google.com/drive/v3/reference/about
        :type fields: list or ", " delimited string
        :return: Dictionary object representation of About resource.
        """
        if fields is None:
            fields = [
                'kind',
                'storageQuota',
                'user'
            ]

        if isinstance(fields, list):
            fields = ', '.join(fields)

        return self._service.about().get(
            fields=fields
        ).execute(num_retries=self._max_retries)

    def list_files(self, max_results=None, **kwargs):
        """
        Abstraction of files().list() method with inbuilt iteration functionality. [https://developers.google.com/drive/v3/reference/files/list]

        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :keyword orderBy: List of keys to sort by. Refer to documentation.
        :keyword spaces: A comma-separated list of spaces to query within the corpus. Supported values are 'drive', 'appDataFolder' and 'photos'.
        :keyword q: A query for filtering the file results. Reference here: https://developers.google.com/drive/v3/web/search-parameters
        :return: List of dictionary objects representing file resources.
        """

        order_by = kwargs.get('orderBy', None)
        if isinstance(order_by, list):
            order_by = ','.join(order_by)

        return iterate_list(
            self._service.files(),
            'files',
            max_results,
            self._max_retries,
            filter_exp=kwargs.get('filter_exp', None),
            orderBy=order_by,
            q=kwargs.get('q', None),
            spaces=kwargs.get('spaces', None)
        )

    def get_file(self, file_id, fields=None):
        """
        Get file metadata.

        :param file_id: Unique file id. Check on UI or by list_files().
        :type file_id: string
        :param fields: Available properties can be found here: https://developers.google.com/drive/v3/reference/about
        :type fields: list or ", " delimited string
        :return: Dictionary object representing file resource.
        """

        if fields is None:
            fields = [
                'name',
                'id',
                'mimeType',
                'modifiedTime',
                'size'
            ]

        if isinstance(fields, list):
            fields = ', '.join(fields)

        resp = self._service.files().get(
            fileId=file_id,
            fields=fields
        ).execute(num_retries=self._max_retries)

        return resp

    def download_file(self, file_id, write_path, page_num=None, output_type=None):
        """
        Downloads object.

        :param file_id: Unique file id. Check on UI or by list_files().
        :type file_id: string
        :param write_path: Local path to write object to.
        :type write_path: string
        :param page_num: Only applicable to Google Sheets. Check **gid** param in URL.
        :type page_num: integer
        :param output_type: Only applicable to Google Sheets. Can be directly downloaded as list or Pandas dataframe.
        :type output_type: string. 'list' or 'dataframe'
        :returns: If Google Sheet and output_type specified: result in selected type, DriveResponse object. Else DriveResponse object.
        :raises: HttpError if non-retryable errors are encountered.
        """

        drive_resp = DriveResponse('downloaded')

        file_metadata = self.get_file(file_id)

        if file_metadata['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            assert page_num is not None

            download_url = 'https://docs.google.com/spreadsheets/d/%s/export?format=csv&gid=%i' % (file_id, page_num)
            resp, content = self._service._http.request(download_url)

            if resp.status == 200:
                if output_type is not None:
                    assert output_type in ('dataframe', 'list')

                    with io.BytesIO(content) as file_buffer:
                        if output_type == 'list':
                            import unicodecsv as csv

                            drive_resp.load_resp(file_metadata, True)
                            return list(csv.reader(file_buffer)), drive_resp

                        elif output_type == 'dataframe':
                            import pandas as pd
                            pd.set_option('display.expand_frame_repr', False)

                            drive_resp.load_resp(file_metadata, True)
                            return pd.read_csv(file_buffer), drive_resp
                else:
                    with io.open(write_path, 'wb') as write_file:
                        write_file.write(content)
            else:
                raise HttpError(resp, content)

        else:
            req = self._service.files().get_media(fileId=file_id)

            with io.open(write_path, 'wb') as write_file:
                downloader = MediaIoBaseDownload(write_file, req)

                done = False
                while done is False:
                    status, done = downloader.next_chunk(num_retries=self._max_retries)

        drive_resp.load_resp(
            file_metadata,
            is_download=True
        )
        return drive_resp

    def upload_file(self, read_path, overwrite_existing=True, **kwargs):
        """
        Creates file if it doesn't exist, updates if it does.

        :param read_path: Local path of object to upload.
        :type read_path: string
        :param overwrite_existing: Safety flag, would raise ValueError if object exists and overwrite_existing=False
        :type overwrite_existing: boolean
        :param kwargs: Key-Value pairs of Request Body params. Reference here: https://developers.google.com/drive/v3/reference/files
        :return: DriveResponse object.
        """
        drive_resp = DriveResponse('uploaded')

        file_name = os.path.basename(read_path)

        request_body = {
            'name': kwargs['name'] if 'name' in kwargs else file_name
        }

        # check for existing file
        if 'name' in kwargs:
            q = 'name="%s"' % kwargs['name']
        else:
            q = 'name="%s"' % file_name

        if 'parents' in kwargs:
            assert isinstance(kwargs['parents'], str)
            q += ' and "%s" in parents' % kwargs['parents']

        existing_files = list(self.list_files(q=q))
        assert len(existing_files) <= 1, 'More than one file matches %s' % file_name

        media = MediaFileUpload(read_path, chunksize=self._chunksize, resumable=True)

        if len(existing_files) == 0:
            if 'parents' in kwargs:
                request_body['parents'] = [kwargs['parents']]

            resp = self._service.files().create(
                media_body=media,
                body=request_body,
                fields='id, name, size, modifiedTime, parents'
            ).execute(num_retries=self._max_retries)

        elif overwrite_existing:
            resp = self._service.files().update(
                fileId=existing_files[0]['id'],
                media_body=media,
                body=request_body,
                fields='id, name, size, modifiedTime, parents'
            ).execute(num_retries=self._max_retries)

        else:
            raise ValueError('Existing file found, set overwrite=True to overwrite file')

        drive_resp.load_resp(
            resp,
            is_download=False
        )
        return drive_resp
