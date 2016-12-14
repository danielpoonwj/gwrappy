from gwrappy.service import get_service
from gwrappy.utils import iterate_list

from itertools import chain
from time import sleep


class ComputeEngineUtility:
    def __init__(self, project_id, **kwargs):
        """
        Initializes object for interacting with Compute Engine API.

        |  By default, Application Default Credentials are used.
        |  If gcloud SDK isn't installed, credential files have to be specified using the kwargs *json_credentials_path* and *client_id*.

        :param project_id: Project ID linked to Compute Engine.
        :keyword max_retries: Argument specified with each API call to natively handle retryable errors.
        :type max_retries: integer
        :keyword client_secret_path: File path for client secret JSON file. Only required if credentials are invalid or unavailable.
        :keyword json_credentials_path: File path for automatically generated credentials.
        :keyword client_id: Credentials are stored as a key-value pair per client_id to facilitate multiple clients using the same credentials file. For simplicity, using one's email address is sufficient.
        """

        self._service = get_service('compute', **kwargs)
        self.project_id = project_id

        self._max_retries = kwargs.get('max_retries', 3)

    def get_project(self):
        """
        Abstraction of projects().get() method. [https://cloud.google.com/compute/docs/reference/latest/projects/get]

        :return: Project Resource
        """

        return self._service.projects().get(
            project=self.project_id
        ).execute(num_retries=self._max_retries)

    def list_regions(self, max_results=None, filter_str=None):
        """
        Abstraction of regions().list() method with inbuilt iteration functionality. [https://cloud.google.com/compute/docs/reference/latest/regions/list]

        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_str: Check documentation link for more details.
        :return: Generator for dictionary objects representing resources.
        """

        return iterate_list(
            self._service.regions(),
            'items',
            max_results,
            self._max_retries,
            project=self.project_id,
            filter=filter_str
        )

    def list_zones(self, max_results=None, filter_str=None):
        """
        Abstraction of zones().list() method with inbuilt iteration functionality. [https://cloud.google.com/compute/docs/reference/latest/zones/list]

        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_str: Check documentation link for more details.
        :return: Generator for dictionary objects representing resources.
        """

        return iterate_list(
            self._service.zones(),
            'items',
            max_results,
            self._max_retries,
            project=self.project_id,
            filter=filter_str
        )

    def list_instances(self, zone_id=None, max_results=None, filter_str=None):
        """
        Abstraction of instances().list() method with inbuilt iteration functionality. [https://cloud.google.com/compute/docs/reference/latest/instances/list]

        :param zone_id: Zone name. If None, all Zones are iterated over and returned.
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_str: Check documentation link for more details.
        :return: Generator for dictionary objects representing resources.
        """

        if zone_id is None:
            return_list = [
                iterate_list(
                    self._service.instances(),
                    'items',
                    max_results,
                    self._max_retries,
                    project=self.project_id,
                    zone=zone['name'],
                    filter=filter_str
                )
                for zone in self.list_zones()
            ]
            return chain(*return_list)

        else:
            return iterate_list(
                self._service.instances(),
                'items',
                max_results,
                self._max_retries,
                project=self.project_id,
                zone=zone_id,
                filter=filter_str
            )

    def list_addresses(self, region_id=None, max_results=None, filter_str=None):
        """
        Abstraction of addresses().list() method with inbuilt iteration functionality. [https://cloud.google.com/compute/docs/reference/latest/addresses/list]

        :param region_id: Region name. If None, all Regions are iterated over and returned.
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_str: Check documentation link for more details.
        :return: Generator for dictionary objects representing resources.
        """

        if region_id is None:
            return_list = [
                iterate_list(
                    self._service.addresses(),
                    'items',
                    max_results,
                    self._max_retries,
                    project=self.project_id,
                    region=region['name'],
                    filter=filter_str
                )
                for region in self.list_regions()
            ]
            return chain(*return_list)

        else:
            return iterate_list(
                self._service.addresses(),
                'items',
                max_results,
                self._max_retries,
                project=self.project_id,
                region=region_id,
                filter=filter_str
            )

    def list_operations(self, operation_type, location_id=None, max_results=None, filter_str=None):
        """
        Choose between region or zone operations with operation_type.

        Abstraction of zoneOperations()/regionOperations().list() method with inbuilt iteration functionality.

        https://cloud.google.com/compute/docs/reference/latest/zoneOperations/list

        https://cloud.google.com/compute/docs/reference/latest/regionOperations/list

        :param operation_type: 'zone' or 'region' type operations.
        :param location_id: Zone/Region name. If None, all Zones/Regions are iterated over and returned.
        :param max_results: If None, all results are iterated over and returned.
        :type max_results: integer
        :param filter_str: Check documentation link for more details.
        :return: Generator for dictionary objects representing resources.
        """

        assert operation_type in ('region', 'zone')

        if location_id is None:
            if operation_type == 'region':
                return_list = [
                    iterate_list(
                        self._service.regionOperations(),
                        'items',
                        max_results,
                        self._max_retries,
                        project=self.project_id,
                        region=region['name'],
                        filter=filter_str
                    )
                    for region in self.list_regions()
                ]

            else:
                return_list = [
                    iterate_list(
                        self._service.zoneOperations(),
                        'items',
                        max_results,
                        self._max_retries,
                        project=self.project_id,
                        zone=zone['name'],
                        filter=filter_str
                    )
                    for zone in self.list_zones()
                ]
            return chain(*return_list)

        else:
            if operation_type == 'region':
                return iterate_list(
                    self._service.regionOperations(),
                    'items',
                    max_results,
                    self._max_retries,
                    project=self.project_id,
                    region=location_id,
                    filter=filter_str
                )

            else:
                return iterate_list(
                    self._service.zoneOperations(),
                    'items',
                    max_results,
                    self._max_retries,
                    project=self.project_id,
                    zone=location_id,
                    filter=filter_str
                )

    def get_operation(self, operation_type, location_id, operation_name):
        """
        Choose between region or zone operations with operation_type.

        Abstraction of zoneOperations()/regionOperations().get() method.

        https://cloud.google.com/compute/docs/reference/latest/zoneOperations/get

        https://cloud.google.com/compute/docs/reference/latest/regionOperations/get

        :param operation_type: 'zone' or 'region' type operations.
        :param location_id: Zone/Region name.
        :param operation_name: Operation name.
        :return: ZoneOperations/RegionOperations Resource.
        """

        assert operation_type in ('region', 'zone')

        if operation_type == 'region':
            return self._service.regionOperations().get(
                project=self.project_id,
                region=location_id,
                operation=operation_name
            ).execute(num_retries=self._max_retries)

        else:
            return self._service.zoneOperations().get(
                project=self.project_id,
                zone=location_id,
                operation=operation_name
            ).execute(num_retries=self._max_retries)

    def poll_operation_status(self, operation_type, location_id, operation_name, end_state, sleep_time=0.5):
        """
        Poll operation to until desired end_state is achieved. eg. 'DONE' when adding addresses.

        :param operation_type: 'zone' or 'region' type operations.
        :param location_id: Zone/Region name.
        :param operation_name: Operation name.
        :param end_state: Final status that signifies operation is finished.
        :param sleep_time: Intervals between polls.
        :return: ZoneOperations/RegionOperations Resource.
        """

        status = None
        resp = None

        while status != end_state:
            resp = self.get_operation(
                operation_type=operation_type,
                location_id=location_id,
                operation_name=operation_name
            )

            status = resp['status']
            sleep(sleep_time)

        return resp

    def get_address(self, region_id, address_name):
        """
        Abstraction of addresses().get() method. [https://cloud.google.com/compute/docs/reference/latest/addresses/get]

        :param region_id: Region name.
        :param address_name: Address name.
        :return: Addresses Resource.
        """

        return self._service.addresses().get(
            project=self.project_id,
            region=region_id,
            address=address_name
        ).execute(num_retries=self._max_retries)

    def add_address(self, region_id, address_name):
        """
        Abstraction of address.insert() method with operation polling functionality. [https://cloud.google.com/compute/docs/reference/latest/addresses/insert]

        :param region_id: Region name.
        :param address_name: Address name.
        :return: RegionOperations Resource.
        """

        resp = self._service.addresses().insert(
            project=self.project_id,
            region=region_id,
            body={'name': address_name}
        ).execute(num_retries=self._max_retries)

        return self.poll_operation_status(
            operation_type='region',
            location_id=region_id,
            operation_name=resp['name'],
            end_state='DONE'
        )

    def delete_address(self, region_id, address_name):
        """
        Abstraction of address.delete() method with operation polling functionality. [https://cloud.google.com/compute/docs/reference/latest/addresses/delete]

        :param region_id: Region name.
        :param address_name: Address name.
        :return: RegionOperations Resource.
        """

        resp = self._service.addresses().delete(
            project=self.project_id,
            region=region_id,
            address=address_name
        ).execute(num_retries=self._max_retries)

        return self.poll_operation_status(
            operation_type='region',
            location_id=region_id,
            operation_name=resp['name'],
            end_state='DONE'
        )

    def get_instance(self, zone_id, instance_name):
        """
        Abstraction of instances().get() method. [https://cloud.google.com/compute/docs/reference/latest/instances/get]

        :param zone_id: Zone name.
        :param instance_name: Instance name.
        :return: Instances Resource.
        """

        return self._service.instances().get(
            project=self.project_id,
            zone=zone_id,
            instance=instance_name
        ).execute(num_retries=self._max_retries)

    def start_instance(self, zone_id, instance_name):
        """
        Abstraction of instances().start() method with operation polling functionality. [https://cloud.google.com/compute/docs/reference/latest/instances/start]

        :param zone_id: Zone name.
        :param instance_name: Instance name.
        :return: ZoneOperations Resource.
        """

        resp = self._service.instances().start(
            project=self.project_id,
            zone=zone_id,
            instance=instance_name
        ).execute(num_retries=self._max_retries)

        return self.poll_operation_status(
            operation_type='zone',
            location_id=zone_id,
            operation_name=resp['name'],
            end_state='DONE'
        )

    def stop_instance(self, zone_id, instance_name):
        """
        Abstraction of instances().stop() method with operation polling functionality. [https://cloud.google.com/compute/docs/reference/latest/instances/stop]

        :param zone_id: Zone name.
        :param instance_name: Instance name.
        :return: ZoneOperations Resource.
        """

        resp = self._service.instances().stop(
            project=self.project_id,
            zone=zone_id,
            instance=instance_name
        ).execute(num_retries=self._max_retries)

        return self.poll_operation_status(
            operation_type='zone',
            location_id=zone_id,
            operation_name=resp['name'],
            end_state='DONE'
        )

    def delete_instance(self, zone_id, instance_name):
        """
        Abstraction of instances().delete() method with operation polling functionality. [https://cloud.google.com/compute/docs/reference/latest/instances/delete]

        :param zone_id: Zone name.
        :param instance_name: Instance name.
        :return: ZoneOperations Resource.
        """

        resp = self._service.instances().delete(
            project=self.project_id,
            zone=zone_id,
            instance=instance_name
        ).execute(num_retries=self._max_retries)

        return self.poll_operation_status(
            operation_type='zone',
            location_id=zone_id,
            operation_name=resp['name'],
            end_state='DONE'
        )
