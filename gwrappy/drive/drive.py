import os

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from gwrappy.service import get_service
from gwrappy.iterator import iterate_list
from gwrappy.errors import HttpError
from gwrappy.drive.utils import DriveResponse


class DriveUtility:
    def __init__(self, json_credentials_path, client_id, **kwargs):
        self._service = get_service('drive', json_credentials_path=json_credentials_path, client_id=client_id, **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

        # Number of bytes to send/receive in each request.
        self._chunksize = kwargs.get('chunksize', 2 * 1024 * 1024)

    def get_account_info(self, fields=None):
        """
        :param fields: check api documentation for accepted fields. can be list or string
        :return: json response
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

        fields = kwargs.get('fields', None)
        if isinstance(fields, list):
            if 'nextPageToken' not in fields:
                fields.append('nextPageToken')
            fields = ', '.join(fields)

        return iterate_list(
            self._service.files(),
            'files',
            max_results,
            self._max_retries,
            filter_exp=None,
            fields=fields,
            q=kwargs.get('q', None),
            spaces=kwargs.get('spaces', None)
        )

    def get_file(self, file_id, fields=None):
        """
        Get file metadata
        :param file_id: unique id for each file. check on UI or by list_files()
        :param fields: check api documentation for accepted fields. can be list or string
        :return: json response
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
        drive_resp = DriveResponse('downloaded')

        file_metadata = self.get_file(file_id)

        if file_metadata['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            assert page_num is not None

            download_url = 'https://docs.google.com/spreadsheets/d/%s/export?format=csv&gid=%i' % (file_id, page_num)
            resp, content = self._service._http.request(download_url)

            if resp.status == 200:
                if output_type is not None:
                    assert output_type in ('dataframe', 'list')

                    from io import BytesIO

                    with BytesIO(content) as file_buffer:
                        if output_type == 'list':
                            import unicodecsv as csv

                            drive_resp.load_resp(file_metadata, True)
                            return list(csv.reader(file_buffer)), drive_resp

                        elif output_type == 'dataframe':
                            import pandas as pd

                            drive_resp.load_resp(file_metadata, True)
                            return pd.read_csv(file_buffer), drive_resp
                else:
                    with open(write_path, 'wb') as write_file:
                        write_file.write(content)
            else:
                raise HttpError(resp, content)

        else:
            req = self._service.files().get_media(fileId=file_id)

            with open(write_path, 'wb') as write_file:
                downloader = MediaIoBaseDownload(write_file, req)

                done = False
                while done is False:
                    status, done = downloader.next_chunk(num_retries=self._max_retries)

        drive_resp.load_resp(file_metadata, True)
        return drive_resp

    def upload_file(self, read_path, overwrite=True, **kwargs):
        """
        Creates file if it doesn't exist, updates if it does
        :param read_path: upload file path
        :param overwrite: safety param to prevent overwriting existing files
        :param kwargs: https://developers.google.com/drive/v3/reference/files/ create or update for valid Request Body kwargs
        :return: json response
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

        existing_files = self.list_files(q=q)
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

        elif overwrite:
            resp = self._service.files().update(
                fileId=existing_files[0]['id'],
                media_body=media,
                body=request_body,
                fields='id, name, size, modifiedTime, parents'
            ).execute(num_retries=self._max_retries)

        else:
            raise ValueError('Existing file found, set overwrite=True to overwrite file')

        drive_resp.load_resp(resp)
        return drive_resp
