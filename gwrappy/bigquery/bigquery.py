from time import sleep

from gwrappy.service import get_service
from gwrappy.utils import iterate_list, datetime_to_timestamp
from gwrappy.errors import HttpError
from gwrappy.bigquery.errors import JobError
from gwrappy.bigquery.utils import JobResponse, TableResponse


class BigqueryUtility:
    def __init__(self, **kwargs):
        """
        Initializes object for interacting with Bigquery API.

        |  By default, Application Default Credentials are used.
        |  If gcloud SDK isn't installed, credential files have to be specified using the kwargs *json_credentials_path* and *client_id*.

        :keyword max_retries: Argument specified with each API call to natively handle retryable errors.
        :type max_retries: integer
        :keyword client_secret_path: File path for client secret JSON file. Only required if credentials are invalid or unavailable.
        :keyword json_credentials_path: File path for automatically generated credentials.
        :keyword client_id: Credentials are stored as a key-value pair per client_id to facilitate multiple clients using the same credentials file. For simplicity, using one's email address is sufficient.
        """

        self._service = get_service('bigquery', **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

    def list_projects(self, max_results=None, filter_exp=None):
        """
        Abstraction of projects().list() method with inbuilt iteration functionality. [https://cloud.google.com/bigquery/docs/reference/v2/projects/list]

        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_exp: Function that filters entries if filter_exp evaluates to True.
        :type filter_exp: function
        :return: List of dictionary objects representing project resources.
        """

        return iterate_list(
            self._service.projects(),
            'projects',
            max_results,
            self._max_retries,
            filter_exp
         )

    def list_jobs(self, project_id, state_filter=None, show_all=False, projection='full', max_results=None, earliest_date=None, filter_exp=None):
        """
        Abstraction of jobs().list() method with inbuilt iteration functionality. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/list]

        **Note** - All jobs are stored in BigQuery. Do set *max_results* or *earliest_date* to limit data returned.

        :param project_id: Unique project identifier.
        :type project_id: string
        :param state_filter: Pre-filter API request for job state. Acceptable values are "done", "pending" and "running". [Equivalent API param: stateFilter]
        :type state_filter: string
        :param show_all: Whether to display jobs owned by all users in the project. [Equivalent API param: allUsers]
        :type show_all: boolean
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param projection: Acceptable values are *'full'*, *'minimal'*. *'full'* includes job configuration.
        :type projection: string
        :param earliest_date: Only returns data after this date.
        :type earliest_date: datetime object or string representation of datetime in %Y-%m-%d format.
        :param filter_exp: Function that filters entries if filter_exp evaluates to True.
        :type filter_exp: function
        :return: List of dictionary objects representing job resources.
        """

        if earliest_date is not None:
            # bigquery timestamp is in milliseconds
            earliest_date_timestamp = datetime_to_timestamp(earliest_date) * 1000
            break_condition = lambda x: int(x['statistics']['creationTime']) < earliest_date_timestamp
        else:
            break_condition = None

        return iterate_list(
            self._service.jobs(),
            'jobs',
            max_results,
            self._max_retries,
            filter_exp,
            break_condition=break_condition,

            projectId=project_id,
            allUsers=show_all,
            projection=projection,
            stateFilter=state_filter
         )

    def list_datasets(self, project_id, show_all=False, max_results=None, filter_exp=None):
        """
        Abstraction of datasets().list() method with inbuilt iteration functionality. [https://cloud.google.com/bigquery/docs/reference/v2/datasets/list]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param show_all: Include hidden datasets generated when running queries on the UI.
        :type show_all: boolean
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_exp: Function that filters entries if filter_exp evaluates to True.
        :type filter_exp: function
        :return: List of dictionary objects representing dataset resources.
        """

        return iterate_list(
            self._service.datasets(),
            'datasets',
            max_results,
            self._max_retries,
            filter_exp,
            projectId=project_id,
            all=show_all
        )

    def list_tables(self, project_id, dataset_id, max_results=None, filter_exp=None):
        """
        Abstraction of tables().list() method with inbuilt iteration functionality. [https://cloud.google.com/bigquery/docs/reference/v2/tables/list]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param dataset_id: Unique dataset identifier.
        :type dataset_id: string
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_exp: Function that filters entries if filter_exp evaluates to True.
        :type filter_exp: function
        :return: List of dictionary objects representing table resources.
        """

        return iterate_list(
            self._service.tables(),
            'tables',
            max_results,
            self._max_retries,
            filter_exp,
            projectId=project_id,
            datasetId=dataset_id
        )

    def get_job(self, project_id, job_id):
        """
        Abstraction of jobs().get() method. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/get]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param job_id: Unique job identifier.
        :type job_id: string
        :return: Dictionary object representing job resource.
        """

        job_resp = self._service.jobs().get(
            projectId=project_id,
            jobId=job_id
        ).execute(num_retries=self._max_retries)

        return job_resp

    def get_table_info(self, project_id, dataset_id, table_id):
        """
        Abstraction of tables().get() method. [https://cloud.google.com/bigquery/docs/reference/v2/tables/get]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param dataset_id: Unique dataset identifier.
        :type dataset_id: string
        :param table_id: Unique table identifier.
        :type table_id: string
        :return: Dictionary object representing table resource.
        """

        return self._service.tables().get(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute(num_retries=self._max_retries)

    def delete_table(self, project_id, dataset_id, table_id):
        """
        Abstraction of tables().delete() method. [https://cloud.google.com/bigquery/docs/reference/v2/tables/delete]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param dataset_id: Unique dataset identifier.
        :type dataset_id: string
        :param table_id: Unique table identifier.
        :type table_id: string
        :raises: AssertionError if unsuccessful. Response should be empty string if successful.
        """

        # if successful, will return empty string

        job_resp = self._service.tables().delete(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute(num_retries=self._max_retries)

        if len(job_resp) > 0:
            raise AssertionError(job_resp)

    def _get_query_results(self, job_resp, page_token=None, max_results=None):
        resp = self._service.jobs().getQueryResults(
            projectId=job_resp['jobReference']['projectId'],
            jobId=job_resp['jobReference']['jobId'],
            maxResults=max_results,
            pageToken=page_token,
            timeoutMs=0
        ).execute(num_retries=self._max_retries)

        return resp

    def poll_job_status(self, job_resp, sleep_time=1):
        """
        Check status of job until status is "DONE".

        :param job_resp: Representation of job resource.
        :type job_resp: dictionary
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :return: Dictionary object representing job resource's final state.
        :raises: JobError object if an error is discovered after job finishes running.
        """

        status_state = None

        while not status_state == 'DONE':
            job_resp = self.get_job(
                project_id=job_resp['jobReference']['projectId'],
                job_id=job_resp['jobReference']['jobId']
            )

            status_state = job_resp['status']['state']
            sleep(sleep_time)

        if 'errorResult' in job_resp['status']:
            raise JobError(job_resp)

        return job_resp

    def _iterate_job_results(self, job_resp, return_type, sleep_time):
        assert return_type in ('list', 'dataframe', 'json')

        job_resp = self.poll_job_status(job_resp, sleep_time)
        results = []
        query_resp = None

        while query_resp is None or 'pageToken' in query_resp:
            page_token = None
            if query_resp is not None:
                page_token = query_resp.get('pageToken', None)

            query_resp = self._get_query_results(
                job_resp,
                page_token=page_token
            )

            # add column names as first row
            if page_token is None:
                results.append([item['name'] for item in query_resp['schema']['fields']])

            # iterate through rows
            rows = query_resp.pop('rows', [])
            for row in rows:
                results.append([item['v'] for item in row['f']])

            sleep(sleep_time)

        # only job_resp from getQueryResults has totalRows field, combined with return_resp
        job_resp[u'totalRows'] = query_resp.get('totalRows', u'0')
        if return_type == 'list':
            return results, job_resp

        # for dataframe and json
        # json will be first converted to dataframe for proper typing
        else:
            import pandas as pd
            pd.set_option('display.expand_frame_repr', False)

            from io import BytesIO
            import unicodecsv as csv

            query_schema = query_resp['schema']['fields']

            def _convert_timestamp(input_value):
                try:
                    return pd.datetime.utcfromtimestamp(float(input_value))
                except (TypeError, ValueError):
                    return pd.np.NaN

            def _convert_dtypes(schema):
                dtype_dict = {
                    'STRING': object,
                    # 'INTEGER': long,
                    'FLOAT': float,
                    'BOOLEAN': bool
                }

                # pandas 0.19.0 would wrongly convert int columns to float if column: None in dtype dict
                return {
                    x['name']: dtype_dict[x['type']] for x in schema if x['type'] in dtype_dict.keys()
                }

            with BytesIO() as file_buffer:
                csv_writer = csv.writer(file_buffer, lineterminator='\n')
                csv_writer.writerows(results)
                file_buffer.seek(0)

                timestamp_cols = [x['name'] for x in query_schema if x['type'] == 'TIMESTAMP']

                results = pd.read_csv(
                    file_buffer,
                    parse_dates=timestamp_cols,
                    date_parser=_convert_timestamp,
                    keep_default_na=False,
                    na_values=['NULL', 'null', ''],
                    dtype=_convert_dtypes(query_schema)
                )

            if return_type == 'dataframe':
                return results, job_resp
            elif return_type == 'json':
                results = results.to_dict('records')
                # pandas will return NaN, convert to native python None
                results = [{k: v if pd.notnull(v) else None for k, v in x.items()} for x in results]
                return results, job_resp

    def sync_query(self, project_id, query, return_type='list', sleep_time=1, dry_run=False, **kwargs):
        """
        Abstraction of jobs().query() method, iterating and parsing query results. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/query]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param query: SQL query
        :type query: string
        :param return_type: Format for result to be returned. Accepted types are "list", "dataframe", and "json".
        :type return_type: string
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :param dry_run: Basic statistics about the query, without actually running it. Mainly for testing or estimating amount of data processed.
        :type dry_run: boolean
        :keyword useLegacySql: Toggle between Legacy and Standard SQL.
        :return: If not dry_run: result in specified type, JobResponse object. If dry_run: Dictionary object representing expected query statistics.
        :raises: JobError object if an error is discovered after job finishes running.
        """

        request_body = {
            'query': query,
            'timeoutMs': 0,
            'dryRun': dry_run,
            'useLegacySql': kwargs.get('useLegacySql', None)
        }

        job_resp = self._service.jobs().query(
            projectId=project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        if not dry_run:
            result, job_resp = self._iterate_job_results(job_resp, return_type, sleep_time)
            return result, JobResponse(job_resp, 'sync')
        else:
            return job_resp

    def async_query(self, project_id, query, dest_project_id, dest_dataset_id, dest_table_id, udf=None,
                    return_type='list', sleep_time=1, **kwargs):
        """
        Abstraction of jobs().insert() method for **query** job, iterating and parsing query results. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/insert]

        Asynchronous queries always write to an intermediate (destination) table.

        |  This query method is preferable over sync_query if:
        |  1. Large results are returned.
        |  2. UDF functions are required.
        |  3. Results returned also need to be stored in a table.

        :param project_id: Unique project identifier.
        :type project_id: string
        :param query: SQL query
        :type query: string
        :param dest_project_id: Unique project identifier of destination table.
        :type dest_project_id: string
        :param dest_dataset_id: Unique dataset identifier of destination table.
        :type dest_dataset_id: string
        :param dest_table_id: Unique table identifier of destination table.
        :type dest_table_id: string
        :param udf: One or more UDF functions if required by the query.
        :type udf: string or list
        :param return_type: Format for result to be returned. Accepted types are "list", "dataframe", and "json".
        :type return_type: string
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :keyword useLegacySql: Toggle between Legacy and Standard SQL.
        :keyword writeDisposition: (Optional) Config kwarg that determines table writing behaviour.
        :return: result in specified type, JobResponse object.
        :raises: JobError object if an error is discovered after job finishes running.
        """

        request_body = {
            'jobReference': {
                'projectId': project_id
            },
            'configuration': {
                'query': {

                    'userDefinedFunctionResources': udf,

                    'query': query,
                    'allowLargeResults': 'true',
                    'destinationTable': {
                        'projectId': dest_project_id,
                        'datasetId': dest_dataset_id,
                        'tableId': dest_table_id,
                    },
                    'writeDisposition': kwargs.get('writeDisposition', 'WRITE_TRUNCATE'),
                    'flattenResults': kwargs.get('flattenResults', None),
                    'useLegacySql': kwargs.get('useLegacySql', None)
                }
            }
        }

        response = self._service.jobs().insert(
            projectId=project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        result, job_resp = self._iterate_job_results(response, return_type, sleep_time)
        return result, JobResponse(job_resp, 'async')

    def write_table(self, project_id, query, dest_project_id, dest_dataset_id, dest_table_id, udf=None,
                    wait_finish=True, sleep_time=1, **kwargs):
        """
        Abstraction of jobs().insert() method for **query** job, without returning results. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/insert]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param query: SQL query
        :type query: string
        :param dest_project_id: Unique project identifier of destination table.
        :type dest_project_id: string
        :param dest_dataset_id: Unique dataset identifier of destination table.
        :type dest_dataset_id: string
        :param dest_table_id: Unique table identifier of destination table.
        :type dest_table_id: string
        :param udf: One or more UDF functions if required by the query.
        :type udf: string or list
        :param wait_finish: Flag whether to poll job till completion. If set to false, multiple jobs can be submitted, repsonses stored, iterated over and polled till completion afterwards.
        :type wait_finish: boolean
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :keyword useLegacySql: Toggle between Legacy and Standard SQL.
        :keyword writeDisposition: (Optional) Config kwarg that determines table writing behaviour.
        :return: If wait_finish: result in specified type, JobResponse object. If not wait_finish: JobResponse object.
        :raises: If wait_finish: JobError object if an error is discovered after job finishes running.
        """

        request_body = {
            'jobReference': {
                'projectId': project_id
            },
            'configuration': {
                'query': {

                    'userDefinedFunctionResources': udf,

                    'query': query,
                    'allowLargeResults': 'true',
                    'destinationTable': {
                        'projectId': dest_project_id,
                        'datasetId': dest_dataset_id,
                        'tableId': dest_table_id,
                    },
                    'writeDisposition': kwargs.get('writeDisposition', 'WRITE_TRUNCATE'),
                    'flattenResults': kwargs.get('flattenResults', None),
                    'useLegacySql': kwargs.get('useLegacySql', None)
                }
            }
        }

        # error would be raised if overwriting a view, check if exists and delete first
        try:
            existing_table = self.get_table_info(dest_project_id, dest_dataset_id, dest_table_id)
            if existing_table['type'] == 'VIEW':
                self.delete_table(
                    dest_project_id,
                    dest_dataset_id,
                    dest_table_id
                )
        except HttpError as e:
            # table does not exist
            if e.resp.status == 404:
                pass
            else:
                raise e

        job_resp = self._service.jobs().insert(
            projectId=project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.poll_job_status(job_resp, sleep_time)

            # additional check to get total_rows
            query_resp = self._get_query_results(
                job_resp,
                max_results=0
            )
            job_resp[u'totalRows'] = query_resp.get('totalRows', u'0')

        return JobResponse(job_resp, 'write table')

    def write_view(self, query, dest_project_id, dest_dataset_id, dest_table_id, udf=None, overwrite_existing=True, **kwargs):
        """
        Views are analogous to a virtual table, functioning as a table but only returning results from the underlying query when called.

        :param query: SQL query
        :type query: string
        :param dest_project_id: Unique project identifier of destination table.
        :type dest_project_id: string
        :param dest_dataset_id: Unique dataset identifier of destination table.
        :type dest_dataset_id: string
        :param dest_table_id: Unique table identifier of destination table.
        :type dest_table_id: string
        :param udf: One or more UDF functions if required by the query.
        :type udf: string or list
        :param overwrite_existing: Safety flag, would raise HttpError if table exists and overwrite_existing=False
        :keyword useLegacySql: Toggle between Legacy and Standard SQL.
        :return: TableResponse object for the newly inserted table
        """

        request_body = {
            'tableReference': {
                'projectId': dest_project_id,
                'datasetId': dest_dataset_id,
                'tableId': dest_table_id
            },
            'view': {
                'userDefinedFunctionResources': udf,
                'query': query,
                'useLegacySql': kwargs.get('useLegacySql', None)
            }
        }

        # error would be raised if table/view already exists, delete first before reinserting
        try:
            job_resp = self._service.tables().insert(
                projectId=dest_project_id,
                datasetId=dest_dataset_id,
                body=request_body
            ).execute(num_retries=self._max_retries)

        except HttpError as e:
            if e.resp.status == 409 and overwrite_existing:
                self.delete_table(
                    dest_project_id,
                    dest_dataset_id,
                    dest_table_id
                )

                job_resp = self._service.tables().insert(
                    projectId=dest_project_id,
                    datasetId=dest_dataset_id,
                    body=request_body
                ).execute(num_retries=self._max_retries)

            else:
                raise e

        return TableResponse(job_resp, 'write')

    def load_from_gcs(self, dest_project_id, dest_dataset_id, dest_table_id, schema, source_uris,
                      wait_finish=True, sleep_time=1, **kwargs):
        """
        |  For loading data from Google Cloud Storage.
        |  Abstraction of jobs().insert() method for **load** job. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/insert]

        :param dest_project_id: Unique project identifier of destination table.
        :type dest_project_id: string
        :param dest_dataset_id: Unique dataset identifier of destination table.
        :type dest_dataset_id: string
        :param dest_table_id: Unique table identifier of destination table.
        :type dest_table_id: string
        :param schema: Schema of input data (schema.fields[]) [https://cloud.google.com/bigquery/docs/reference/v2/tables]
        :type schema: list of dictionaries
        :param source_uris: One or more uris referencing GCS objects
        :type source_uris: string or list
        :param wait_finish: Flag whether to poll job till completion. If set to false, multiple jobs can be submitted, repsonses stored, iterated over and polled till completion afterwards.
        :type wait_finish: boolean
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :keyword writeDisposition: Determines table writing behaviour.
        :keyword sourceFormat: Indicates format of input data.
        :keyword skipLeadingRows: Leading rows to skip. Defaults to 1 to account for headers if sourceFormat is CSV or default, 0 otherwise.
        :keyword fieldDelimiter: Indicates field delimiter.
        :keyword allowQuotedNewlines: Indicates presence of quoted newlines in fields.
        :keyword allowJaggedRows: Accept rows that are missing trailing optional columns. (Only CSV)
        :keyword ignoreUnknownValues: Allow extra values that are not represented in the table schema.
        :keyword maxBadRecords: Maximum number of bad records that BigQuery can ignore when running the job.
        :return: JobResponse object
        """

        request_body = {
            'jobReference': {
                'projectId': dest_project_id
            },

            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': dest_project_id,
                        'datasetId': dest_dataset_id,
                        'tableId': dest_table_id
                    },
                    'writeDisposition': kwargs.get('writeDisposition', 'WRITE_TRUNCATE'),
                    'sourceFormat': kwargs.get('sourceFormat', None),
                    'skipLeadingRows': kwargs.get('skipLeadingRows', 1) if kwargs.get('sourceFormat', None) in (None, 'CSV') else None,
                    'fieldDelimiter': kwargs.get('fieldDelimiter', None),
                    'schema': {
                        'fields': schema
                    },
                    'sourceUris': source_uris if isinstance(source_uris, list) else [source_uris],
                    'allowQuotedNewlines': kwargs.get('allowQuotedNewlines', None),
                    'allowJaggedRows': kwargs.get('allowJaggedRows', None),
                    'ignoreUnknownValues': kwargs.get('ignoreUnknownValues', None),
                    'maxBadRecords': kwargs.get('maxBadRecords', None)
                }
            }
        }

        job_resp = self._service.jobs().insert(
            projectId=dest_project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.poll_job_status(job_resp, sleep_time)

        return JobResponse(job_resp, 'gcs')

    def export_to_gcs(self, source_project_id, source_dataset_id, source_table_id, dest_uris,
                      wait_finish=True, sleep_time=1, **kwargs):
        """
        |  For exporting data into Google Cloud Storage.
        |  Abstraction of jobs().insert() method for **extract** job. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/insert]

        :param source_project_id: Unique project identifier of source table.
        :type source_project_id: string
        :param source_dataset_id: Unique dataset identifier of source table.
        :type source_dataset_id: string
        :param source_table_id: Unique table identifier of source table.
        :type source_table_id: string
        :param dest_uris: One or more uris referencing GCS objects
        :type dest_uris: string or list
        :param wait_finish: Flag whether to poll job till completion. If set to false, multiple jobs can be submitted, repsonses stored, iterated over and polled till completion afterwards.
        :type wait_finish: boolean
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :keyword destinationFormat: (Optional) Config kwarg that indicates format of output data.
        :keyword compression: (Optional) Config kwarg for type of compression applied.
        :keyword fieldDelimiter: (Optional) Config kwarg that indicates field delimiter.
        :keyword printHeader: (Optional) Config kwarg indicating if table headers should be written.
        :return: JobResponse object
        """

        request_body = {
            'jobReference': {
                'projectId': source_project_id
            },

            'configuration': {
                'extract': {
                    'sourceTable': {
                        'projectId': source_project_id,
                        'datasetId': source_dataset_id,
                        'tableId': source_table_id
                    },
                    'destinationUris': dest_uris if isinstance(dest_uris, list) else [dest_uris],

                    'destinationFormat': kwargs.get('destinationFormat', None),
                    'compression': kwargs.get('compression', None),
                    'fieldDelimiter': kwargs.get('fieldDelimiter', None),
                    'printHeader': kwargs.get('printHeader', None),
                }
            }
        }

        job_resp = self._service.jobs().insert(
            projectId=source_project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.poll_job_status(job_resp, sleep_time)

        return JobResponse(job_resp)

    def copy_table(self, source_data, dest_project_id, dest_dataset_id, dest_table_id,
                   wait_finish=True, sleep_time=1, **kwargs):
        """
        |  For copying existing table(s) to a new or existing table.
        |  Abstraction of jobs().insert() method for **copy** job. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/insert]

        :param source_data: Representations of single or multiple existing tables to copy from.
        :param source_date: dictionary or list of dictionaries
        :param dest_project_id: Unique project identifier of destination table.
        :type dest_project_id: string
        :param dest_dataset_id: Unique dataset identifier of destination table.
        :type dest_dataset_id: string
        :param dest_table_id: Unique table identifier of destination table.
        :type dest_table_id: string
        :param wait_finish: Flag whether to poll job till completion. If set to false, multiple jobs can be submitted, repsonses stored, iterated over and polled till completion afterwards.
        :type wait_finish: boolean
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :keyword writeDisposition: (Optional) Config kwarg that determines table writing behaviour.
        :return: JobResponse object
        """

        required_keys = ['datasetId', 'projectId', 'tableId']

        if isinstance(source_data, dict):
            assert list(sorted(source_data.keys())) == required_keys
            source_data = [source_data]
        else:
            assert isinstance(source_data, list)
            assert all([isinstance(x, dict) for x in source_data])
            assert all([list(sorted(x.keys())) == required_keys for x in source_data])

        request_body = {
            'jobReference': {
                'projectId': dest_project_id
            },

            'configuration': {
                'copy': {
                    'destinationTable': {
                        'projectId': dest_project_id,
                        'datasetId': dest_dataset_id,
                        'tableId': dest_table_id
                    },
                    'sourceTables': source_data,
                    'writeDisposition': kwargs.get('writeDisposition', 'WRITE_TRUNCATE')
                }
            }
        }

        job_resp = self._service.jobs().insert(
            projectId=dest_project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.poll_job_status(job_resp, sleep_time)

        return JobResponse(job_resp)

    def load_from_string(self, dest_project_id, dest_dataset_id, dest_table_id, schema, load_string,
                         wait_finish=True, sleep_time=1, **kwargs):
        """
        |  For loading data from string representation of a file/object.
        |  Can be used in conjunction with gwrappy.bigquery.utils.file_to_string()

        :param dest_project_id: Unique project identifier of destination table.
        :type dest_project_id: string
        :param dest_dataset_id: Unique dataset identifier of destination table.
        :type dest_dataset_id: string
        :param dest_table_id: Unique table identifier of destination table.
        :type dest_table_id: string
        :param schema: Schema of input data (schema.fields[]) [https://cloud.google.com/bigquery/docs/reference/v2/tables]
        :type schema: list of dictionaries
        :param load_string: String representation of an object.
        :type load_string: string
        :param wait_finish: Flag whether to poll job till completion. If set to false, multiple jobs can be submitted, repsonses stored, iterated over and polled till completion afterwards.
        :type wait_finish: boolean
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :keyword writeDisposition: (Optional) Config kwarg that determines table writing behaviour.
        :keyword sourceFormat: (Optional) Config kwarg that indicates format of input data.
        :keyword skipLeadingRows: (Optional) Config kwarg for leading rows to skip. Defaults to 1 to account for headers if sourceFormat is CSV or default, 0 otherwise.
        :keyword fieldDelimiter: (Optional) Config kwarg that indicates field delimiter.
        :keyword allowQuotedNewlines: (Optional) Config kwarg indicating presence of quoted newlines in fields.
        :keyword allowJaggedRows: Accept rows that are missing trailing optional columns. (Only CSV)
        :keyword ignoreUnknownValues: Allow extra values that are not represented in the table schema.
        :keyword maxBadRecords: Maximum number of bad records that BigQuery can ignore when running the job.
        :return: JobResponse object
        """

        from googleapiclient.http import MediaInMemoryUpload

        request_body = {
            'jobReference': {
                'projectId': dest_project_id
            },

            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': dest_project_id,
                        'datasetId': dest_dataset_id,
                        'tableId': dest_table_id
                    },
                    'writeDisposition': kwargs.get('writeDisposition', 'WRITE_TRUNCATE'),
                    'sourceFormat': kwargs.get('sourceFormat', None),
                    'skipLeadingRows': kwargs.get('skipLeadingRows', 1) if kwargs.get('sourceFormat', None) in (None, 'CSV') else None,
                    'fieldDelimiter': kwargs.get('fieldDelimiter', None),
                    'schema': {
                        'fields': schema
                    },
                    'allowQuotedNewlines': kwargs.get('allowQuotedNewlines', None),
                    'allowJaggedRows': kwargs.get('allowJaggedRows', None),
                    'ignoreUnknownValues': kwargs.get('ignoreUnknownValues', None),
                    'maxBadRecords': kwargs.get('maxBadRecords', None)
                }
            }
        }

        media_body = MediaInMemoryUpload(load_string, mimetype='application/octet-stream')

        job_resp = self._service.jobs().insert(
            projectId=dest_project_id,
            body=request_body,
            media_body=media_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.poll_job_status(job_resp, sleep_time)

        return JobResponse(job_resp, 'string')

    def write_federated_table(self, dest_project_id, dest_dataset_id, dest_table_id, schema, source_uris,
                              overwrite_existing=True, **kwargs):
        """
        |  Imagine a View for Google Cloud Storage object(s).
        |  Abstraction of jobs().insert() method for **load** job. [https://cloud.google.com/bigquery/docs/reference/v2/jobs/insert]

        :param dest_project_id: Unique project identifier of destination table.
        :type dest_project_id: string
        :param dest_dataset_id: Unique dataset identifier of destination table.
        :type dest_dataset_id: string
        :param dest_table_id: Unique table identifier of destination table.
        :type dest_table_id: string
        :param schema: Schema of input data (schema.fields[]) [https://cloud.google.com/bigquery/docs/reference/v2/tables]
        :type schema: list of dictionaries
        :param source_uris: One or more uris referencing GCS objects
        :type source_uris: string or list
        :param overwrite_existing: Safety flag, would raise HttpError if table exists and overwrite_existing=False
        :keyword sourceFormat: (Optional) Config kwarg that indicates format of input data.
        :keyword skipLeadingRows: (Optional) Config kwarg for leading rows to skip. Defaults to 1 to account for headers if sourceFormat is CSV or default, 0 otherwise.
        :keyword fieldDelimiter: (Optional) Config kwarg that indicates field delimiter.
        :keyword compression: (Optional) Config kwarg for type of compression applied.
        :keyword allowQuotedNewlines: (Optional) Config kwarg indicating presence of quoted newlines in fields.
        :return: TableResponse object
        """


        request_body = {
            'tableReference': {
                'projectId': dest_project_id,
                'datasetId': dest_dataset_id,
                'tableId': dest_table_id
            },
            'externalDataConfiguration': {
                'sourceUris': source_uris if isinstance(source_uris, list) else [source_uris],
                'schema': {
                    'fields': schema
                },
                'sourceFormat': kwargs.get('sourceFormat', None),
                'compression': kwargs.get('sourceFormat', None),

                'csvOptions': {
                    'skipLeadingRows': kwargs.get('skipLeadingRows', 1) if kwargs.get('sourceFormat', None) in (None, 'CSV') else None,
                    'fieldDelimiter': kwargs.get('fieldDelimiter', None),
                    'allowQuotedNewlines': kwargs.get('allowQuotedNewlines', None)
                }
            }
        }

        # error would be raised if table/view already exists, delete first before reinserting
        try:
            job_resp = self._service.tables().insert(
                projectId=dest_project_id,
                datasetId=dest_dataset_id,
                body=request_body
            ).execute(num_retries=self._max_retries)

        except HttpError as e:
            if e.resp.status == 409 and overwrite_existing:
                self.delete_table(
                    dest_project_id,
                    dest_dataset_id,
                    dest_table_id
                )

                job_resp = self._service.tables().insert(
                    projectId=dest_project_id,
                    datasetId=dest_dataset_id,
                    body=request_body
                ).execute(num_retries=self._max_retries)

            else:
                raise e

        return TableResponse(job_resp, 'write federated')

    def update_table_info(self, project_id, dataset_id, table_id, table_description=None, schema=None):
        """
        Abstraction of tables().patch() method. [https://cloud.google.com/bigquery/docs/reference/v2/tables/patch]

        :param project_id: Unique project identifier.
        :type project_id: string
        :param dataset_id: Unique dataset identifier.
        :type dataset_id: string
        :param table_id: Unique table identifier.
        :type table_id: string
        :param table_description: Optional description for table. If None, would not overwrite existing description.
        :type table_description: string
        :param schema_fields:
        :param schema: Schema fields to change (schema.fields[]) [https://cloud.google.com/bigquery/docs/reference/v2/tables]
        :type schema: list of dictionaries
        :return: TableResponse
        """

        request_body = {
            'tableReference': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id
            }
        }

        if table_description is not None:
            request_body['description'] = table_description

        if schema is not None:
            assert isinstance(schema, list)
            assert all([isinstance(x, dict) for x in schema])

            original_fields = self.get_table_info(
                project_id,
                dataset_id,
                table_id
            )['schema']['fields']

            # all fields have to be supplied even with table patch, this method checks and updates original fields
            # this method won't support adding new fields to prevent potentially accidentally adding etc
            # checks that all supplied schema fields are already existing fields
            assert all([schema_field['name'] in [x['name'] for x in original_fields] for schema_field in schema])

            for schema_field in schema:
                for original_field in original_fields:
                    if schema_field['name'] == original_field['name']:
                        original_field.update(schema_field)
                        break

            request_body['schema'] = {
                'fields': original_fields
            }

        job_resp = self._service.tables().patch(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        return TableResponse(job_resp)

    def poll_resp_list(self, response_list, sleep_time=1):
        """
        Convenience function for iterating and polling list of responses collected with jobs wait_finish=False.

        If any job fails, its respective Error object is returned to ensure errors would not break polling subsequent responses.

        :param response_list: List of response objects
        :type response_list: list of dicts or JobResponse objects
        :param sleep_time: Time to pause (seconds) between polls.
        :type sleep_time: integer
        :return: List of JobResponse or Error (JobError/HttpError) objects representing job resource's final state.
        """
        # to use when setting wait_finish = False to insert jobs without waiting
        # store initial responses in list and use this method to iterate and poll responses
        assert isinstance(response_list, (list, tuple, set))

        return_list = []

        for response in response_list:
            try:
                if isinstance(response, JobResponse):
                    resp = self.poll_job_status(response.resp, sleep_time)
                    return_list.append(JobResponse(resp, getattr(response, 'description', None)))
                else:
                    assert isinstance(response, dict)
                    resp = self.poll_job_status(response, sleep_time)
                    return_list.append(JobResponse(resp))
            except (JobError, HttpError) as e:
                return_list.append(e)

        return return_list
