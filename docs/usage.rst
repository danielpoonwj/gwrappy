=====
Usage
=====

This package is designed to take the pain out of using Google's powerful APIs. Focus on using them instead of getting them to work.

Each API is accessible through a specific Utility found within each subpackage.
These Utilities are objects that initiate and authenticate the service objects required to access the each API's functionality.

Authentication
--------------

It is **highly** recommended to download the wonderful gcloud SDK [https://cloud.google.com/sdk/] as a complementary tool.
For one, it allows you to simplify access to Google Cloud Platform services using Application Default Credentials.
Otherwise, credentials would have to be authenticated and a credentials file would have to be stored etc etc.

If the gcloud SDK has been installed and configured to the desired user and project, authentication is seamless.

.. code-block:: python

    from gwrappy.bigquery import BigqueryUtility
    bq_obj = BigqueryUtility()

Application Default Credentials are only applicable for services under the Google Cloud Platform. For other services, such as Gmail or Drive, unfortunately the process is *slightly* less elegant.
Also, if multiple user credentials are required, this solution may actually be more convenient.
Authentication flow with client_secret.json is only required once per service, thereafter the credentials are stored as a value in credentials.json, with the key being the client_id.

.. code-block:: python

    from gwrappy.bigquery import BigqueryUtility
    secret_path = path/to/client_secret.json
    cred_path = path/to/credentials.json
    client_id = me@gmail.com

    bq_obj = BigqueryUtility(
        client_secret_path=secret_path,
        json_credentials_path=cred_path,
        client_id=client_id
    )

Class Methods
-------------

Once the Utility object has been initialized, accessing methods within the object are generally wrapped APi calls.
For more information on accepted kwargs, please visit the respective method's documentation.

Working with Response Objects
-----------------------------

Some methods return Response objects eg. gwrappy.bigquery.utils.JobResponse.
These objects are generally parsing the JSON responses from the API, calculating statistics like time taken and size (if applicable) and converting it to a human-readable format.

Should the original API response be required for custom logging or other reasons, access the **resp** variable within the Response object.
