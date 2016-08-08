from time import sleep

from gwrappy.service import get_service
from gwrappy.iterator import iterate_list
from gwrappy.errors import JobError, HttpError


class BigqueryUtility:
    def __init__(self, **kwargs):
        self._service = get_service('bigquery', **kwargs)

        self._max_retries = kwargs.get('max_retries', 3)

    def list_projects(self, max_results=None, filter_exp=None):
        return iterate_list(
            self._service.projects(),
            'projects',
            max_results,
            self._max_retries,
            filter_exp
         )

    def list_jobs(self, project_id, state_filter=None, show_all=False, max_results=None, filter_exp=None):
        return iterate_list(
            self._service.jobs(),
            'jobs',
            max_results,
            self._max_retries,
            filter_exp,
            projectId=project_id,
            allUsers=show_all,
            stateFilter=state_filter
         )

    def list_datasets(self, project_id, show_all=False, max_results=None, filter_exp=None):
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
        return self._service.jobs().get(
            projectId=project_id,
            jobId=job_id
        ).execute(num_retries=self._max_retries)

    def get_table_info(self, project_id, dataset_id, table_id):
        return self._service.tables().get(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute(num_retries=self._max_retries)

    def delete_table(self, project_id, dataset_id, table_id):
        return self._service.tables().delete(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute(num_retries=self._max_retries)

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
        status_state = None

        while not status_state == 'DONE':
            job_resp = self._service.jobs().get(
                projectId=job_resp['jobReference']['projectId'],
                jobId=job_resp['jobReference']['jobId']
            ).execute(num_retries=self._max_retries)

            status_state = job_resp['status']['state']
            sleep(sleep_time)

        if 'errorResult' in job_resp['status']:
            raise JobError(job_resp)

        return job_resp

    def _iterate_job_results(self, job_resp, return_type, sleep_time):
        assert return_type in ('list', 'dataframe')

        job_resp = self.poll_job_status(job_resp, sleep_time)
        results_list = []
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
                results_list.append([item['name'] for item in query_resp['schema']['fields']])

            # iterate through rows
            rows = query_resp.pop('rows', [])
            for row in rows:
                results_list.append([item['v'] for item in row['f']])

            sleep(sleep_time)

        # only job_resp from getQueryResults has totalRows field, combined with return_resp
        job_resp[u'totalRows'] = query_resp.get('totalRows', u'0')
        if return_type == 'list':
            return results_list, job_resp

        elif return_type == 'dataframe':
            import pandas as pd
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
                    'INTEGER': long,
                    'FLOAT': float,
                    'BOOLEAN': bool
                }

                return {
                    x['name']: dtype_dict[x['type']] for x in schema if x['type'] != 'TIMESTAMP'
                }

            with BytesIO() as file_buffer:
                csv_writer = csv.writer(file_buffer, lineterminator='\n')
                csv_writer.writerows(results_list)
                file_buffer.seek(0)

                timestamp_cols = [x['name'] for x in query_schema if x['type'] == 'TIMESTAMP']

                results_df = pd.read_csv(
                    file_buffer,
                    parse_dates=timestamp_cols,
                    date_parser=_convert_timestamp,
                    keep_default_na=False,
                    na_values=['NULL', 'null', ''],
                    dtype=_convert_dtypes(query_schema)
                )

            return results_df, job_resp

    def sync_query(self, project_id, query, return_type='list', sleep_time=1):
        request_body = {
            'query': query,
            'timeoutMs': 0
        }

        job_resp = self._service.jobs().query(
            projectId=project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        return self._iterate_job_results(job_resp, return_type, sleep_time)

    def async_query(self, project_id, query, dest_project_id, dest_dataset_id, dest_table_id, udf=None,
                    return_type='list', sleep_time=1, **kwargs):

        request_body = {
            'jobReference': {
                'projectId': project_id
            },
            'configuration': {
                'query': {

                    'userDefinedFunctionResources': udf,

                    'query': query,
                    'allowLargeResults': kwargs.get('allowLargeResults', 'true'),
                    'destinationTable': {
                        'projectId': dest_project_id,
                        'datasetId': dest_dataset_id,
                        'tableId': dest_table_id,
                    },
                    'writeDisposition': kwargs.get('writeDisposition', 'WRITE_TRUNCATE')
                }
            }
        }

        response = self._service.jobs().insert(
            projectId=project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        return self._iterate_job_results(response, return_type, sleep_time)

    def write_table(self, project_id, query, dest_project_id, dest_dataset_id, dest_table_id, udf=None,
                    wait_finish=True, sleep_time=1, **kwargs):

        request_body = {
            'jobReference': {
                'projectId': project_id
            },
            'configuration': {
                'query': {

                    'userDefinedFunctionResources': udf,

                    'query': query,
                    'allowLargeResults': kwargs.get('allowLargeResults', 'true'),
                    'destinationTable': {
                        'projectId': dest_project_id,
                        'datasetId': dest_dataset_id,
                        'tableId': dest_table_id,
                    },
                    'writeDisposition': kwargs.get('writeDisposition', 'WRITE_TRUNCATE'),
                    'flattenResults': kwargs.get('flattenResults', None)
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

        return job_resp

    def write_view(self, query, dest_project_id, dest_dataset_id, dest_table_id, udf=None, overwrite_existing=True):

        request_body = {
            'tableReference': {
                'projectId': dest_project_id,
                'datasetId': dest_dataset_id,
                'tableId': dest_table_id
            },
            'view': {
                'userDefinedFunctionResources': udf,
                'query': query
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

        return job_resp

    def load_from_gcs(self, dest_project_id, dest_dataset_id, dest_table_id, schema, source_uris,
                      wait_finish=True, sleep_time=1, **kwargs):

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
                    'skipLeadingRows': kwargs.get('skipLeadingRows', 1) if kwargs.get('sourceFormat', None) in (None, 'CSV') else kwargs.get('skipLeadingRows', 0),
                    'fieldDelimiter': kwargs.get('fieldDelimiter', None),
                    'schema': {
                        'fields': schema
                    },
                    'sourceUris': source_uris if isinstance(source_uris, list) else [source_uris],
                    'allowQuotedNewlines': kwargs.get('allowQuotedNewlines', None)
                }
            }
        }

        job_resp = self._service.jobs().insert(
            projectId=dest_project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.poll_job_status(job_resp, sleep_time)

        return job_resp

    def export_to_gcs(self, source_project_id, source_dataset_id, source_table_id, dest_uris,
                      wait_finish=True, sleep_time=1, **kwargs):

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

        return job_resp

    def copy_table(self, source_data, dest_project_id, dest_dataset_id, dest_table_id,
                   wait_finish=True, sleep_time=1, **kwargs):

        """
        can copy multiple source tables to the same destination
        source_data can be a list of dicts or a single dict
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
                'projectId': dest_project_id['projectId']
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

        return job_resp

    def load_from_string(self, dest_project_id, dest_dataset_id, dest_table_id, schema, load_string,
                         wait_finish=True, sleep_time=1, **kwargs):

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
                    'skipLeadingRows': kwargs.get('skipLeadingRows', 1) if kwargs.get('sourceFormat', None) in (None, 'CSV') else kwargs.get('skipLeadingRows', 0),
                    'fieldDelimiter': kwargs.get('fieldDelimiter', None),
                    'schema': {
                        'fields': schema
                    },
                    'allowQuotedNewlines': kwargs.get('allowQuotedNewlines', None)
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

        return job_resp

    def write_federated_table(self, dest_project_id, dest_dataset_id, dest_table_id, schema, source_uris,
                              overwrite_existing=True, **kwargs):

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
                    'skipLeadingRows': kwargs.get('skipLeadingRows', 1) if kwargs.get('sourceFormat', None) in (None, 'CSV') else kwargs.get('skipLeadingRows', 0),
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

        return job_resp

    def update_table_info(self, project_id, dataset_id, table_id, table_description=None, schema_fields=None):
        request_body = {
            'tableReference': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id
            }
        }

        if table_description is not None:
            request_body['description'] = table_description

        if schema_fields is not None:
            assert isinstance(schema_fields, list)
            assert all([isinstance(x, dict) for x in schema_fields])

            original_fields = self.get_table_info(
                project_id,
                dataset_id,
                table_id
            )['schema']['fields']

            # all fields have to be supplied even with table patch, this method checks and updates original fields
            # this method won't support adding new fields to prevent potentially accidentally adding etc
            # checks that all supplied schema fields are already existing fields
            assert all([schema_field['name'] in [x['name'] for x in original_fields] for schema_field in schema_fields])

            for schema_field in schema_fields:
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

        return job_resp
