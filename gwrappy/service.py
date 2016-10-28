from __future__ import absolute_import
from googleapiclient.discovery import build
from .scopes import SCOPES


def get_service(service_name, **kwargs):
    service_scope = SCOPES[service_name]

    if 'json_credentials_path' in kwargs:
        # store in multistore_file by default, requires client_id as a key
        assert 'client_id' in kwargs, 'client_id required when using json_credential_path'

        import httplib2
        from oauth2client.contrib import multistore_file

        storage = multistore_file.get_credential_storage(
            filename=kwargs['json_credentials_path'],
            client_id=kwargs['client_id'],
            user_agent=None,
            scope=service_scope['scope']
        )

        credentials = storage.get()

        if credentials is None or credentials.invalid:
            # rerun auth flow if credentials are missing or invalid
            # flow requires client secret file
            assert 'client_secret_path' in kwargs, 'Credentials invalid, client_secret_path required for reauthorization'

            from oauth2client.client import flow_from_clientsecrets
            from oauth2client.tools import run_flow

            FLOW = flow_from_clientsecrets(kwargs['client_secret_path'], scope=service_scope['scope'])
            credentials = run_flow(FLOW, storage, None)

        # Create an httplib2.Http object and authorize it with your credentials
        http = httplib2.Http()
        http = credentials.authorize(http)

        return build(
            service_name,
            service_scope['version'],
            http=http
        )

    else:
        from oauth2client.client import GoogleCredentials
        credentials = GoogleCredentials.get_application_default()

        return build(
            service_name,
            service_scope['version'],
            credentials=credentials
        )
