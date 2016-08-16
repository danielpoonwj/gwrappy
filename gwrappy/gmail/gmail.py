from gwrappy.service import get_service
from gwrappy.utils import iterate_list, timestamp_to_datetime
from gwrappy.gmail.utils import create_message

import base64


class GmailUtility:
    def __init__(self, json_credentials_path, client_id, **kwargs):
        """
        Initializes object for interacting with Bigquery API.

        :param client_secret_path: File path for client secret JSON file. Only required if credentials are invalid or unavailable.
        :param json_credentials_path: File path for automatically generated credentials.
        :param client_id: Credentials are stored as a key-value pair per client_id to facilitate multiple clients using the same credentials file. For simplicity, using one's email address is sufficient.
        :keyword max_retries: Argument specified with each API call to natively handle retryable errors.
        :type max_retries: integer
        """

        self._service = get_service('gmail', json_credentials_path=json_credentials_path, client_id=client_id, **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

    def get_profile(self):
        """
        Abstraction of users().getProfile() method. [https://developers.google.com/gmail/api/v1/reference/users/getProfile]

        :return: Dictionary object representing authenticated profile.
        """

        return self._service.users().getProfile(userId='me').execute(num_retries=self._max_retries)

    def get_message(self, id, format='full'):
        """
        Abstraction of users().messages().get() method. [https://developers.google.com/gmail/api/v1/reference/users/messages/get]

        :argument id: Unique message id.
        :type id: string
        :keyword format: Acceptable values are 'full', 'metadata', 'minimal', 'raw'
        :type format: string
        :return: Dictionary object representing message resource.
        """

        return self._service.users().messages().get(
            id=id,
            userId='me',
            format=format
        ).execute(num_retries=self._max_retries)

    def get_draft(self, id, format='full'):
        """
        Abstraction of users().drafts().get() method. [https://developers.google.com/gmail/api/v1/reference/users/drafts/get]

        :argument id: Unique message id.
        :type id: string
        :keyword format: Acceptable values are 'full', 'metadata', 'minimal', 'raw'
        :type format: string
        :return: Dictionary object representing draft resource.
        """

        return self._service.users().drafts().get(
            id=id,
            userId='me',
            format=format
        ).execute(num_retries=self._max_retries)

    def list_messages(self, max_results=None, full_messages=True, **kwargs):
        """
        Abstraction of users().messages().list() method with inbuilt iteration functionality. [https://developers.google.com/gmail/api/v1/reference/users/messages/list]

        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param full_messages: Convenience toggle to call self.get_message() for each message returned.
        :type full_messages: boolean
        :keyword q: A query for filtering the file results. Can be generated from gwrappy.gmail.utils.generate_q
        :return: List of dictionary objects representing message resources.
        """

        kwargs['userId'] = 'me'

        results = iterate_list(
            self._service.users().messages(),
            'messages',
            max_results,
            self._max_retries,
            **kwargs
         )

        if full_messages:
            results = [self.get_message(x['id']) for x in results]

        return results

    def list_drafts(self, max_results=None, full_messages=True, **kwargs):
        """
        Abstraction of users().drafts().list() method with inbuilt iteration functionality. [https://developers.google.com/gmail/api/v1/reference/users/drafts/list]

        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param full_messages: Convenience toggle to call self.get_draft() for each message returned.
        :type full_messages: boolean
        :keyword q: A query for filtering the file results. Can be generated from gwrappy.gmail.utils.generate_q
        :return: List of dictionary objects representing draft resources.
        """

        kwargs['userId'] = 'me'

        results = iterate_list(
            self._service.users().drafts(),
            'drafts',
            max_results,
            self._max_retries,
            **kwargs
        )

        if full_messages:
            results = [self.get_draft(x['id']) for x in results]

        return results

    def create_draft(self, sender, to, subject, message_text, attachment_file_paths=None):
        """
        New draft based on input parameters.

        :param sender: Name of sender
        :type sender: string
        :param to: One or more recipients.
        :type to: string or list
        :param subject: Subject text
        :param message_text: Message string, or one or more dict representations of message parts. If dict, keys required are **type** and **text**.
        :type message_text: string, dict, or list of dicts
        :param attachment_file_paths: One or more file paths of attachments.
        :type attachment_file_paths: string or list
        :return: API response.
        """

        message = {'message': create_message(sender, to, subject, message_text, attachment_file_paths)}
        resp = self._service.users().drafts().create(
            userId='me',
            body=message
        ).execute(num_retries=self._max_retries)

        return resp

    def send_draft(self, draft_id):
        """
        Send unsent draft.

        :param draft_id: Unique draft id.
        :return: API Response.
        """

        resp = self._service.users().drafts().send(
            userId='me',
            body={'id': draft_id}
        ).execute(num_retries=self._max_retries)

        return resp

    def send_email(self, sender, to, subject, message_text, attachment_file_paths=None):
        """
        Send new message based on input parameters.

        :param sender: Name of sender
        :type sender: string
        :param to: One or more recipients.
        :type to: string or list
        :param subject: Subject text
        :param message_text: Message string, or one or more dict representations of message parts. If dict, keys required are **type** and **text**.
        :type message_text: string, dict, or list of dicts
        :param attachment_file_paths: One or more file paths of attachments.
        :type attachment_file_paths: string or list
        :return: API response.
        """

        message = create_message(sender, to, subject, message_text, attachment_file_paths)
        resp = self._service.users().messages().send(
            userId='me',
            body=message
        ).execute(num_retries=self._max_retries)

        return resp

    def get_attachments(self, message_id):
        """
        Get message attachments.

        :param message_id: Unique message id. Can be retrieved and iterated over from list_messages() method.
        :return: Dictionary with parsed dates and attachment_data (ready to write to file!). Duplicate handling and overwriting logic **should** be handled externally when iterating over list of messages.
        """

        def _get_attachment_data(msg_id, att_id):
            return self._service.users().messages().attachments().get(
                id=att_id,
                messageId=msg_id,
                userId='me'
            ).execute(num_retries=self._max_retries)

        def _list_attachments(obj, key, parts_list):
            if isinstance(obj, dict):
                try:
                    parts_list.append(
                        {
                            'file_name': obj['filename'],
                            'attachment_id': obj['body']['attachmentId'],
                            'mime_type': obj['mimeType']
                        }
                    )
                except KeyError:
                    pass

            if key in obj and isinstance(obj[key], list):
                for part in obj[key]:
                    try:
                        parts_list.append(
                            {
                                'file_name': part['filename'],
                                'attachment_id': part['body']['attachmentId'],
                                'mime_type': part['mimeType']
                            }
                        )
                    except KeyError:
                        pass

                    if key in part:
                        _list_attachments(part, key, parts_list)

        message = self.get_message(message_id)

        attachments = []
        _list_attachments(message['payload'], 'parts', attachments)

        for attachment in attachments:
            assert isinstance(attachment, dict)

            attachment['date'] = timestamp_to_datetime(message['internalDate'])
            attachment['message_id'] = message_id

            attachment_data = _get_attachment_data(message_id, attachment['attachment_id'])
            attachment['attachment_data'] = base64.urlsafe_b64decode(attachment_data['data'].encode('utf-8'))

        return attachments
