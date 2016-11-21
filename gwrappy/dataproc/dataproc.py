from gwrappy.service import get_service
from gwrappy.utils import iterate_list

from time import sleep


class DataprocUtility:
    def __init__(self, **kwargs):
        """
        Initializes object for interacting with Dataproc API.

        |  By default, Application Default Credentials are used.
        |  If gcloud SDK isn't installed, credential files have to be specified using the kwargs *json_credentials_path* and *client_id*.

        :keyword client_secret_path: File path for client secret JSON file. Only required if credentials are invalid or unavailable.
        :keyword json_credentials_path: File path for automatically generated credentials.
        :keyword client_id: Credentials are stored as a key-value pair per client_id to facilitate multiple clients using the same credentials file. For simplicity, using one's email address is sufficient.
        """

        self._service = get_service('dataproc', **kwargs)
        self._max_retries = kwargs.get('max_retries', 3)

    def list_clusters(self, project_id, max_results=None, filter=None):
        """
        Abstraction of projects().regions().clusters().list() method with inbuilt iteration functionality. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/list]

        :param project_id: GCP Project that the cluster belongs to.
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter: Query param [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/list#query-parameters]
        :type filter: String
        :return: List of dictionary objects representing cluster resources.
        """

        return iterate_list(
            self._service.projects().regions().clusters(),
            'clusters',
            max_results,
            self._max_retries,
            projectId=project_id,
            region='global',
            filter=filter
         )

    def get_cluster(self, project_id, cluster_name):
        """
        Abstraction of projects().regions().clusters().get() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/get]

        :param project_id: GCP Project that the cluster belongs to.
        :param cluster_name: Cluster name.
        :return: Dictionary object representing cluster resource.
        """

        cluster_resp = self._service.projects().regions().clusters().get(
            projectId=project_id,
            region='global',
            clusterName=cluster_name
        ).execute(num_retries=self._max_retries)

        return cluster_resp

    def diagnose_cluster(self, project_id, cluster_name):
        """
        Abstraction of projects().regions().clusters().diagnose() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/diagnose]

        :param project_id: GCP Project that the cluster belongs to.
        :param cluster_name: Cluster name.
        :return: Dictionary object representing operation resource.
        """

        cluster_resp = self._service.projects().regions().clusters().diagnose(
            projectId=project_id,
            region='global',
            clusterName=cluster_name,
            body={}
        ).execute(num_retries=self._max_retries)

        return cluster_resp

    def list_operations(self, project_id, max_results=None, filter=None):
        """
        Abstraction of projects().regions().operations().list() method with inbuilt iteration functionality. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.operations/list]

        :param project_id: GCP Project that the cluster belongs to.
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter: Query param [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.operations/list#query-parameters]
        :type filter: String
        :return: List of dictionary objects representing operation resources.
        """

        return iterate_list(
            self._service.projects().regions().operations(),
            'operations',
            max_results,
            self._max_retries,
            name='projects/{project_id}/regions/{region}/operations'.format(project_id=project_id, region='global'),
            filter=filter
         )

    def get_operation(self, name, wait_finish=True, sleep_time=1):
        """
        Abstraction of projects().regions().operations().get() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.operations/get]

        :param name: Name of operation resource.
        :param wait_finish: If set to True, operation will be polled till completion.
        :param sleep_time: If wait_finish is set to True, sets polling wait time.
        :return: Dictionary object representing operation resource.
        """

        operation_resp = None
        is_complete = False

        while not is_complete:
            operation_resp = self._service.projects().regions().operations().get(
                name=name
            ).execute(num_retries=self._max_retries)

            if wait_finish:
                is_complete = operation_resp.get('done', False) or \
                              operation_resp['metadata']['status']['state'] == 'DONE'

                if not is_complete:
                    sleep(sleep_time)
            else:
                is_complete = True

        return operation_resp

    def create_cluster(self, project_id, zone, cluster_name, wait_finish=True, **kwargs):
        """
        Abstraction of projects().regions().clusters().create() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/create]

        :param project_id: GCP Project that the cluster belongs to.
        :param zone: Dataproc zone.
        :param cluster_name: Cluster name.
        :param wait_finish: If set to True, operation will be polled till completion.
        :keyword config_bucket: Google Cloud Storage staging bucket used for sharing generated SSH keys and config.
        :keyword network: Google Compute Engine network to be used for machine communications.

        :keyword master_boot_disk: Size in GB of the master boot disk.
        :keyword master_num: The number of master VM instances in the instance group.
        :keyword master_machine_type: Google Compute Engine machine type used for master cluster instances.

        :keyword worker_boot_disk: Size in GB of the worker boot disk.
        :keyword worker_num: The number of VM worker instances in the instance group.
        :keyword worker_machine_type: Google Compute Engine machine type used for worker cluster instances.

        :keyword init_actions: Google Cloud Storage URI of executable file(s).
        :return: Dictionary object representing cluster resource.
        """

        init_actions = kwargs.get('init_actions', [])
        if not isinstance(init_actions, list):
            init_actions = [init_actions]

        cluster_body = {
            'clusterName': cluster_name,
            'projectId': project_id,
            'config': {
                'configBucket': kwargs.get('config_bucket', ''),
                'gceClusterConfig': {
                    'networkUri': 'https://www.googleapis.com/compute/v1/projects/{project_id}/global/networks/{network}'.format(
                        project_id=project_id,
                        network=kwargs.get('network', 'default')
                    ),
                    'zoneUri': 'https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{zone}'.format(
                        project_id=project_id,
                        zone=zone
                    )
                },
                'masterConfig': {
                    'numInstances': kwargs.get('master_num', 1),
                    'machineTypeUri': 'https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{zone}/machineTypes/{master_machine_type}'.format(
                        project_id=project_id,
                        zone=zone,
                        master_machine_type=kwargs.get('master_machine_type', 'n1-standard-4')
                    ),
                    'diskConfig': {
                        'bootDiskSizeGb': kwargs.get('master_boot_disk', 500),
                        'numLocalSsds': 0
                    }
                },
                'workerConfig': {
                    'numInstances': kwargs.get('worker_num', 2),
                    'machineTypeUri': 'https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{zone}/machineTypes/{worker_machine_type}'.format(
                        project_id=project_id,
                        zone=zone,
                        worker_machine_type=kwargs.get('worker_machine_type', 'n1-standard-4')
                    ),
                    'diskConfig': {
                        'bootDiskSizeGb': kwargs.get('worker_boot_disk', 500),
                        'numLocalSsds': 0
                    }
                },
                'initializationActions': [{'executableFile': x} for x in init_actions]
            }
        }

        cluster_resp = self._service.projects().regions().clusters().create(
            projectId=project_id,
            region='global',
            body=cluster_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            cluster_resp = self.get_operation(cluster_resp['name'])

        return cluster_resp

    def delete_cluster(self, project_id, cluster_name, wait_finish=True):
        """
        Abstraction of projects().regions().clusters().delete() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/delete]

        :param project_id: GCP Project that the cluster belongs to.
        :param cluster_name: Cluster name.
        :param wait_finish: If set to True, operation will be polled till completion.
        :return: Dictionary object representing cluster resource.
        """

        cluster_resp = self._service.projects().regions().clusters().delete(
            projectId=project_id,
            region='global',
            clusterName=cluster_name
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            cluster_resp = self.get_operation(cluster_resp['name'])

        return cluster_resp

    def list_jobs(self, project_id, cluster_name=None, job_state='ACTIVE', max_results=None, filter=None):
        """
        Abstraction of projects().regions().jobs().list() method with inbuilt iteration functionality. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs/list]

        :param project_id: GCP Project.
        :param cluster_name: Cluster name, if unset, will return jobs from all clusters.
        :param job_state: Category of jobs to return. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs/list#JobStateMatcher]
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter: Query param [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs/list#query-parameters]
        :type filter: String
        :return: List of dictionary objects representing cluster resources.
        """

        return iterate_list(
            self._service.projects().regions().jobs(),
            'jobs',
            max_results,
            self._max_retries,
            projectId=project_id,
            region='global',
            clusterName=cluster_name,
            jobStateMatcher=job_state,
            filter=filter
        )

    def get_job(self, project_id, job_id, wait_finish=True, sleep_time=3):
        """
        Abstraction of projects().regions().jobs().get() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs/get]

        :param project_id: GCP Project that the cluster belongs to.
        :param job_id: Job Id.
        :param wait_finish: If set to True, operation will be polled till completion.
        :param sleep_time: If wait_finish is set to True, sets polling wait time.
        :return: Dictionary object representing cluster resource.
        """

        job_resp = None
        is_complete = False

        while not is_complete:
            job_resp = self._service.projects().regions().jobs().get(
                projectId=project_id,
                region='global',
                jobId=job_id
            ).execute(num_retries=self._max_retries)

            if wait_finish:
                is_complete = job_resp['status']['state'] == 'DONE'
                if not is_complete:
                    sleep(sleep_time)
            else:
                is_complete = True

        return job_resp

    def submit_spark_job(self, project_id, cluster_name, main_class, wait_finish=True, sleep_time=5, **kwargs):
        """
        Abstraction of projects().regions().jobs().submit() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs/submit]

        Body parameters can be found here: [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs#resource-job]

        :param project_id: GCP Project that the cluster belongs to.
        :param cluster_name: The name of the cluster where the job will be submitted.
        :param main_class: The name of the driver's main class.
        :param wait_finish: If set to True, operation will be polled till completion.
        :param sleep_time: If wait_finish is set to True, sets polling wait time.
        :keyword args: The arguments to pass to the driver.
        :keyword jar_uris: HCFS URIs of jar files to add to the CLASSPATHs of the Spark driver and tasks.
        :keyword file_uris: HCFS URIs of files to be copied to the working directory of Spark drivers and distributed tasks.
        :keyword archive_uris: HCFS URIs of archives to be extracted in the working directory of Spark drivers and tasks.
        :keyword properties: A mapping of property names to values, used to configure Spark.
        :return: Dictionary object representing cluster resource.
        """

        # validate fields
        assert isinstance(kwargs.get('args', []), list)
        assert isinstance(kwargs.get('jar_uris', []), list)
        assert isinstance(kwargs.get('file_uris', []), list)
        assert isinstance(kwargs.get('archive_uris', []), list)
        assert isinstance(kwargs.get('properties', {}), dict)

        submit_body = {
            'job': {
                'placement': {
                    'clusterName': cluster_name
                },
                'sparkJob': {
                    'mainClass': main_class,
                    'args': kwargs.get('args', []),
                    'jarFileUris': kwargs.get('jar_uris', []),
                    'fileUris': kwargs.get('file_uris', []),
                    'archiveUris': kwargs.get('archive_uris', []),
                    'properties': kwargs.get('properties', {})
                }
            }
        }

        job_resp = self._service.projects().regions().jobs().submit(
            projectId=project_id,
            region='global',
            body=submit_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.get_job(
                job_resp['reference']['projectId'],
                job_resp['reference']['jobId'],
                wait_finish=True,
                sleep_time=sleep_time
            )

        return job_resp

    def submit_pyspark_job(self, project_id, cluster_name, main_py_uri, wait_finish=True, sleep_time=5, **kwargs):
        """
        Abstraction of projects().regions().jobs().submit() method. [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs/submit]

        Body parameters can be found here: [https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs#resource-job]

        :param project_id: GCP Project that the cluster belongs to.
        :param cluster_name: The name of the cluster where the job will be submitted.
        :param main_py_uri: The HCFS URI of the Python file to use as the driver.
        :param wait_finish: If set to True, operation will be polled till completion.
        :param sleep_time: If wait_finish is set to True, sets polling wait time.
        :keyword args: The arguments to pass to the driver.
        :keyword python_uris: HCFS file URIs of Python files to pass to the PySpark framework.
        :keyword jar_uris: HCFS URIs of jar files to add to the CLASSPATHs of the Python driver and tasks.
        :keyword file_uris: HCFS URIs of files to be copied to the working directory of Python drivers and distributed tasks.
        :keyword archive_uris: HCFS URIs of archives to be extracted in the working directory of Spark drivers and tasks.
        :keyword properties: A mapping of property names to values, used to configure PySpark.
        :return: Dictionary object representing cluster resource.
        """

        # validate fields
        assert isinstance(kwargs.get('args', []), list)
        assert isinstance(kwargs.get('python_uris', []), list)
        assert isinstance(kwargs.get('jar_uris', []), list)
        assert isinstance(kwargs.get('file_uris', []), list)
        assert isinstance(kwargs.get('archive_uris', []), list)
        assert isinstance(kwargs.get('properties', {}), dict)

        submit_body = {
            'placement': {
                'clusterName': cluster_name
            },
            'job': {
                'pysparkJob': {
                    'mainPythonFileUri': main_py_uri,
                    'args': kwargs.get('args', []),
                    'pythonFileUris': kwargs.get('python_uris', []),
                    'jarFileUris': kwargs.get('jar_uris', []),
                    'fileUris': kwargs.get('file_uris', []),
                    'archiveUris': kwargs.get('archive_uris', []),
                    'properties': kwargs.get('properties', {})
                }
            }
        }

        job_resp = self._service.projects().regions().jobs().submit(
            projectId=project_id,
            region='global',
            body=submit_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            job_resp = self.get_job(
                job_resp['reference']['projectId'],
                job_resp['reference']['jobId'],
                sleep_time=sleep_time
            )

        return job_resp
