=======
History
=======

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
