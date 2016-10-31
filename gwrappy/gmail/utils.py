import os
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
import mimetypes
from email.utils import COMMASPACE
import base64

from gwrappy.utils import datetime_to_timestamp


def generate_q(**kwargs):
    """
    Generate query for searching messages. [https://support.google.com/mail/answer/7190]

    :keyword kwargs: Key-Value pairs. Descriptive flags like *has* or *is* can take lists. Otherwise, list values would be interpreted as "OR".
    :return: String representation of search q
    """

    parsed_q = []

    for k, v in kwargs.items():
        if k in ('has', 'is'):
            if isinstance(v, list):
                for element in v:
                    parsed_q.append('%s:%s' % (k, element))
            else:
                parsed_q.append('%s:%s' % (k, v))

        elif k in ('before', 'after'):
            parsed_q.append('%s:%s' % (k, datetime_to_timestamp(v)))

        else:
            if isinstance(v, list):
                parsed_q.append(
                    ' OR '.join(['%s:%s' % (k, element) for element in v])
                )
            else:
                parsed_q.append('%s:%s' % (k, v))

    return None if len(parsed_q) == 0 else ' '.join(parsed_q)


def create_message(sender, to, subject, message_text, attachment_paths=None):
    def __generate_msg_part(part):
        assert isinstance(part, dict)
        assert 'text' in part

        if 'type' not in part:
            msg_type = 'plain'
        else:
            msg_type = part['type']

        mime_part = MIMEText(
            part['text'].encode('utf-8'),
            _subtype=msg_type,
            _charset='utf-8'
        )

        return mime_part

    if not isinstance(to, list):
        to = [to]

    if message_text is None or isinstance(message_text, (unicode, str)):
        msgs = [MIMEText(message_text.encode('utf-8'), _charset='utf-8')]

    elif isinstance(message_text, dict):
        msgs = [__generate_msg_part(message_text)]

    elif isinstance(message_text, list):
        msgs = [__generate_msg_part(message_part) for message_part in message_text]

    else:
        raise TypeError('Types accepted for message_text: string, dict, or list of dicts')

    message = MIMEMultipart()
    message['to'] = COMMASPACE.join(to)
    message['from'] = sender
    message['subject'] = subject

    # separate part for text etc
    message_alt = MIMEMultipart('alternative')
    message.attach(message_alt)

    # html portions have to be under a separate 'related' part under 'alternative' part
    # sequence matters, text > related (html > inline image) > attachments. ascending priority
    # if message text is a list, it's providing alternatives.
    # eg. if both plain and html are available, Gmail will choose HTML over plain

    # attach text first (lower priority)
    for msg in msgs:
        if msg.get_content_subtype() == 'plain':
            message_alt.attach(msg)

    # create 'related' part if html is required
    content_msgs = filter(lambda x: x.get_content_subtype() == 'html' or x.get_content_maintype() == 'image', msgs)

    if len(content_msgs) > 0:
        message_related = MIMEMultipart('related')
        message_alt.attach(message_related)

        for msg in content_msgs:
            message_related.attach(msg)

    # different treatment if contains attachments
    if attachment_paths is not None:
        if isinstance(attachment_paths, str):
            attachment_paths = [attachment_paths]

        elif not isinstance(attachment_paths, list):
            raise TypeError('Invalid input. Only acceptable types are str and list objects')

        for file_path in attachment_paths:
            content_type, encoding = mimetypes.guess_type(file_path)

            if content_type is None or encoding is not None:
                content_type = 'application/octet-stream'

            main_type, sub_type = content_type.split('/', 1)

            with open(file_path, 'rb') as fp:
                if main_type == 'text':
                    msg = MIMEText(fp.read(), _subtype=sub_type)

                elif main_type == 'image':
                    msg = MIMEImage(fp.read(), _subtype=sub_type)

                else:
                    msg = MIMEBase(main_type, sub_type)
                    msg.set_payload(fp.read())

            msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
            message.attach(msg)

    return {'raw': base64.urlsafe_b64encode(message.as_string())}


def list_to_html(data, has_header=True, table_format=None):
    """
    Convenience function to convert tables to html for attaching as message text.

    :param data: Table data
    :type data: list of lists
    :param has_header: Flag whether data contains a header in the first row.
    :type has_header: boolean
    :param table_format: Dictionary representation of formatting for table elements. Eg. {'table': "border: 2px solid black;"}
    :type table_format: dictionary
    :return: String representation of HTML.
    """

    from tabulate import tabulate

    if has_header:
        header = data.pop(0)
    else:
        header = ()

    table_html = tabulate(data, headers=header, tablefmt='html', numalign='left')

    if table_format is not None:
        if isinstance(table_format, str) and table_format.lower() == 'default':
            table_format = {
                'table': "width: 100%; border-collapse: collapse; border: 2px solid black;",
                'th': "border: 2px solid black;",
                'td': "border: 1px solid black;"
            }

        if isinstance(table_format, dict):
            assert all([key in ('table', 'th', 'tr', 'td') for key in table_format.keys()])

            for k, v in table_format.items():
                table_html = table_html.replace('<%s>' % k, '<%s style="%s">' % (k, v))

    return table_html
