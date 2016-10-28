import sys
import os
import io
from datetime import datetime
import humanize


class JobResponse:
    def __init__(self, resp, description=None):
        """
        Wrapper for Bigquery job responses, mainly for calculating/parsing job statistics into human readable formats for logging.

        :param resp: Dictionary representation of a job resource.
        :type resp: dictionary
        :param description: Optional string descriptor for specific function of job.
        """

        assert isinstance(resp, dict)
        assert resp['kind'].split('#')[-1] == 'job'
        assert len(resp['configuration'].keys()) == 1

        self.resp = resp

        if description is not None:
            self.description = description.strip().title()

        self.id = self.resp['id']

        self.job_type = list(resp['configuration'].keys())[0]
        self._parse_job()

    def _parse_job(self):
        try:
            setattr(
                self,
                'time_taken',
                dict(zip(
                    ('m', 's'),
                    divmod(
                        (
                            datetime.utcfromtimestamp(float(self.resp['statistics']['endTime']) / 1000) -
                            datetime.utcfromtimestamp(float(self.resp['statistics']['creationTime']) / 1000)
                        ).seconds,
                        60)
                ))
            )
        except KeyError:
            pass

        if self.job_type == 'load':
            try:
                setattr(self, 'size', humanize.naturalsize(int(self.resp['statistics']['load']['inputFileBytes'])))
            except (KeyError, TypeError):
                pass
        elif self.job_type == 'query':
            try:
                setattr(self, 'size', humanize.naturalsize(int(self.resp['statistics']['query']['totalBytesProcessed'])))
            except (KeyError, TypeError):
                pass

        if self.job_type == 'load':
            try:
                setattr(self, 'row_count', int(self.resp['statistics'][self.job_type]['outputRows']))
            except (KeyError, TypeError):
                pass
        elif self.job_type == 'query':
            try:
                setattr(self, 'row_count', int(self.resp['totalRows']))
            except (KeyError, TypeError):
                pass

    def __repr__(self):
        return '[BigQuery] %s%s Job (%s) %s%s%s' % (
            '%s ' % self.description if hasattr(self, 'description') else '',
            self.job_type.capitalize(),
            self.id,
            '%s rows ' % getattr(self, 'row_count') if hasattr(self, 'row_count') else '',
            '%s processed ' % getattr(self, 'size') if hasattr(self, 'size') else '',
            '({m} Minutes {s} Seconds)'.format(**getattr(self, 'time_taken')) if hasattr(self, 'time_taken') else ''
        )
    
    __str__ = __repr__


class TableResponse:
    def __init__(self, resp, description=None):
        """
        Wrapper for Bigquery table resources, mainly for calculating/parsing job statistics into human readable formats for logging.

        :param resp: Dictionary representation of a table resource.
        :type resp: dictionary
        :param description: Optional string descriptor for table.
        """

        assert isinstance(resp, dict)
        assert resp['kind'].split('#')[-1] == 'table'

        self.resp = resp
        if description is not None:
            self.description = description.strip().title()

        try:
            setattr(self, 'row_count', int(self.resp['numRows']))
        except (KeyError, TypeError):
            pass

        try:
            setattr(self, 'size', humanize.naturalsize(int(self.resp['numBytes'])))
        except (KeyError, TypeError):
            pass

    def __repr__(self):
        return '[BigQuery] %s%s (%s) %s%s' % (
            '%s ' % self.description if hasattr(self, 'description') else '',
            self.resp['type'].capitalize(),
            self.resp['id'],
            '%s rows ' % getattr(self, 'row_count') if hasattr(self, 'row_count') else '',
            '(%s)' % getattr(self, 'size') if hasattr(self, 'size') else ''
        )

    __str__ = __repr__


def read_sql(read_path, **kwargs):
    """
    Reads text file, performing string substitution using str.format() method if necessary.

    :param read_path: File path containing SQL query.
    :param kwargs: Key-Value pairs referencing {key} within query for substitution.
    :return: Query string.
    """

    assert os.path.exists(read_path)

    with io.open(read_path, 'r', encoding='utf-8') as read_file:
        read_string = read_file.read()

        if len(kwargs) > 0:
            read_string = read_string.format(**kwargs)

    return read_string


def bq_schema_from_df(input_df):
    """
    Derive Bigquery Schema from Pandas Dataframe object.

    :param input_df: Pandas Dataframe object
    :return: List of dictionaries which can be fed directly as Bigquery schemas.
    """

    dtype_df = input_df.dtypes.reset_index(drop=False)
    dtype_df = dtype_df.rename(columns={'index': 'name', 0: 'type'})

    dtype_conversion_dict = {
        'b': 'BOOLEAN',
        'i': 'INTEGER',
        'u': 'INTEGER',
        'f': 'FLOAT',
        'c': 'FLOAT',
        'O': 'STRING',
        'S': 'STRING',
        'U': 'STRING',
        'M': 'TIMESTAMP'
    }

    dtype_df['type'] = dtype_df['type'].map(lambda x: dtype_conversion_dict[x.kind])
    return dtype_df.to_dict('records')


def file_to_string(f, source_format='csv'):
    """
    Specifically for BigqueryUtility().load_from_string()

    :param f: Object to convert to string.
    :type f: file path, list of lists/dicts, dataframe, or string representation of json list
    :param source_format: Indicates format of input data. Accepted values are "csv" and "json".
    :type source_format: string
    :return: String representation of object/file contents
    """

    assert source_format.lower() in ('csv', 'json')

    # python 2/3 compatibility
    if sys.version_info.major < 3:
        output_buffer = io.BytesIO()
    else:
        output_buffer = io.StringIO()

    if source_format == 'csv':
        import pandas as pd

        if sys.version_info.major < 3:
            import unicodecsv as csv
        else:
            import csv

        string_writer = csv.writer(output_buffer, lineterminator='\n')

        # string inputs should only be file paths
        if isinstance(f, str):
            assert os.path.exists(f)

            with io.open(f, 'rb') if sys.version_info.major < 3 else io.open(f, 'r', encoding='utf-8') as read_file:
                string_writer.writerows(csv.reader(read_file))

        elif isinstance(f, pd.DataFrame):
            f.to_csv(output_buffer, index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')

        # also accepts list of lists
        elif isinstance(f, list) and all(isinstance(x, list) for x in f):
            string_writer.writerows(f)

        else:
            raise TypeError('Unrecognized type: %s' % type(f))

    elif source_format == 'json':
        import json

        # can be loaded from file path or string in a json structure
        if isinstance(f, str):
            if os.path.exists(f):
                with io.open(f, 'r', encoding='utf-8') as read_file:
                    json_obj = json.load(read_file)
            else:
                json_obj = json.loads(f)

        else:
            try:
                json.dumps(f)
                json_obj = f
            except TypeError as e:
                raise e

        assert isinstance(json_obj, list)

        for index, obj in enumerate(json_obj):
            if index < len(json_obj) - 1:
                output_buffer.write(json.dumps(obj) + '\n')
            else:
                output_buffer.write(json.dumps(obj))

    return_string = output_buffer.getvalue()
    output_buffer.close()

    return return_string
