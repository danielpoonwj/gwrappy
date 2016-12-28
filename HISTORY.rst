=======
History
=======

0.4.3 (2016-12-16)
------------------
* Bugfix in gwrappy.drive.DriveUtility.upload_file()

0.4.2 (2016-12-16)
------------------
* Bugfix in gwrappy.dataproc.DataprocUtility.delete_cluster()

0.4.1 (2016-12-14)
------------------
* Breaking Changes:
    * gwrappy.dataproc.DataprocUtility takes project_id in its constructor rather than a method parameter.
* gwrappy.dataproc.DataprocUtility operation/job methods return Response objects when wait_finish=True
* pandas dependency removed from requirements.txt as its functionality is limited to specific functions and largely unnecessary otherwise.

0.4.0 (2016-11-21)
------------------
* Added gwrappy.dataproc for Google Dataproc
* Minor Changes
    * gwrappy.storage.GcsUtility.update_object() added
    * Added ability to set object Acl on upload with gwrappy.storage.GcsUtility.upload_object()

0.3.0 (2016-10-31)
------------------
* Python 3 compatibility
    * Most API functions were already compatible, most changes were done for the utilities functions.
* Minor Bugfixes/Changes
    * BigqueryUtility().poll_resp_list() now doesn't break once an exception is encountered. The respective Error object is returned and job checking is uninterrupted.
    * Fixed int columns being interpreted as float for pandas 0.19.0 when querying to dataframe.

0.2.1 (2016-10-20)
------------------
* Minor Bugfixes:
    * bigquery.utils.read_sql properly checks kwargs.
    * BigqueryUtility queries with return_type='dataframe' uses inferred dtypes for integer columns to stop pandas from breaking if column contains NaN.

0.2.0 (2016-09-27)
------------------
* Added gwrappy.compute for Google Compute Engine.
* Minor Bugfixes:
    * drive.DriveUtility.list_files(): Removed fields, added orderBy and filter_exp.
    * bigquery.utils.JobResponse: time_taken in __repr__ for some job types fixed.

0.1.6 (2016-09-08)
------------------
* Added more utilities
    * utils.month_range: Chunk dates into months.
    * utils.simple_mail: Send basic emails for alerts or testing. *Note*: For greater security and flexibility, do still use the gmail functionality within this package.
    * utils.StringLogger: Simply wrapper for logging with a string handler and convenience functions for retrieving logs as a string.
* Added dateutil as a dependency

0.1.5 (2016-08-30)
------------------
* list methods now return a generator for memory efficiency
* BigQuery:
    * list_jobs takes 2 new args *projection* and *earliest_date*
* Documentation updates

0.1.4 (2016-08-29)
------------------
* gwrappy.errors no longer imports service specific error objects. To access JobError, import it from gwrappy.bigquery.errors
* simple date range generator function added to gwrappy.utils

0.1.3 (2016-08-23)
------------------
* BigQuery:
    * JobResponse now only sets time_taken if data is available.
        * Fixed bug that raised KeyError when wait_finish=False, since endTime was unavailable in the API response.
    * poll_resp_list returns JobReponse objects. Also propagates 'description' attribute if available.

0.1.2 (2016-08-19)
------------------
* Bug Fixes
* Documentation updates

0.1.1 (2016-08-16)
------------------
* Completed docstrings and amendments to documentation
* Added list_to_html under gwrappy.gmail.utils
* Added tabulate as a dependency

0.1.0 (2016-08-15)
------------------
* New and improved version of https://github.com/danielpoonwj/gcloud_custom_utilities
* First release on PyPI.
