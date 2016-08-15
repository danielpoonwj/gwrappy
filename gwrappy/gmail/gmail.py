from gwrappy.service import get_service
from gwrappy.utils import iterate_list, timestamp_to_datetime
from gwrappy.gmail.utils import create_message

import base64


class GmailUtility:
    def __init__(self, json_credentials_path, client_id, **kwargs):
        self._service = get_service('gmail', json_credentials_path=json_credentials_path, client_id=client_id, **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

    def get_profile(self):
        return self._service.users().getProfile(userId='me').execute(num_retries=self._max_retries)

    def get_message(self, id, format='full'):
        return self._service.users().messages().get(
            id=id,
            userId='me',
            format=format
        ).execute(num_retries=self._max_retries)

    def get_draft(self, id, format='full'):
        return self._service.users().drafts().get(
            id=id,
            userId='me',
            format=format
        ).execute(num_retries=self._max_retries)

    def list_messages(self, max_results=None, full_messages=True, **kwargs):
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
        message = {'message': create_message(sender, to, subject, message_text, attachment_file_paths)}
        resp = self._service.users().drafts().create(
            userId='me',
            body=message
        ).execute(num_retries=self._max_retries)

        return resp

    def send_draft(self, draft_id):
        resp = self._service.users().drafts().send(
            userId='me',
            body={'id': draft_id}
        ).execute(num_retries=self._max_retries)

        return resp

    def send_email(self, sender, to, subject, message_text, attachment_file_paths=None):
        message = create_message(sender, to, subject, message_text, attachment_file_paths)
        resp = self._service.users().messages().send(
            userId='me',
            body=message
        ).execute(num_retries=self._max_retries)

        return resp

    def get_attachments(self, message_id):
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
