=======
gwrappy
=======


.. image:: https://img.shields.io/pypi/v/gwrappy.svg
    :target: https://pypi.python.org/pypi/gwrappy

.. image:: https://readthedocs.org/projects/gwrappy/badge/?version=latest
    :target: https://gwrappy.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


User friendly wrapper for Google APIs.


Features
--------

* Easily connect to the following Google APIs (more to come eventually)
    * BigQuery
    * Cloud Storage
    * Drive
    * Gmail
    * Compute Engine

.. code-block:: python

    # BigQuery
    from gwrappy.bigquery import BigqueryUtility
    bq_obj = BigqueryUtility()
    results = bq_obj.sync_query('my_project', 'SELECT * FROM [foo.bar]')

    # Cloud Storage
    from gwrappy.storage import GcsUtility
    gcs_obj = GcsUtility()
    gcs_obj.download_object('bucket_name', 'object_name', 'path/to/write')
    gcs_obj.upload_object('bucket_name', 'object_name', 'path/to/read')

    # Drive
    from gwrappy.drive import DriveUtility
    drive_obj = DriveUtility(json_credentials_path, client_id)
    drive_obj.download_object('file_id', 'path/to/write')
    drive_obj.upload_file('path/to/read')

    # Gmail
    from gwrappy.gmail import GmailUtility
    gmail_obj = GmailUtility(json_credentials_path, client_id)
    gmail_obj.send_email(sender='Daniel Poon', to=['recipient_1@xx.com', 'recipient_2@yy.com'], subject='Hello World!', message_text='My First Email')


Installation
------------

.. code-block:: bash

    $ pip install gwrappy
