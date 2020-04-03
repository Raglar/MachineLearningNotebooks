from azureml.core.dataset import Dataset
from azureml.core.run import Run
from azureml.core import Datastore, Workspace
from azureml.datadrift import DataDriftDetector, AlertConfiguration
from azureml.core.authentication import InteractiveLoginAuthentication, ServicePrincipalAuthentication
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.widgets import RunDetails

import argparse
from pprint import pprint
from pathlib import Path
import json
import os
import shutil
from datetime import datetime


def get_auth(tenant_id, app_id, client_secret):
    try:
        return ServicePrincipalAuthentication(
            tenant_id=tenant_id,
            service_principal_id=app_id,
            service_principal_password=client_secret
        )
    except KeyError:
        raise Exception('Error getting Service Principal Authentication')


def create_datadrift_compute(ws):
    # Verify that cluster does not exist already
    cpu_cluster_name = 'reddit-test'
    try:
        cpu_cluster = ComputeTarget(workspace=ws, name=cpu_cluster_name)
        print('Found existing cluster, use it.')
    except ComputeTargetException:
        compute_config = AmlCompute.provisioning_configuration(vm_size='STANDARD_D2_V2',
                                                            max_nodes=4)
        cpu_cluster = ComputeTarget.create(ws, cpu_cluster_name, compute_config)

    cpu_cluster.wait_for_completion(show_output=True)
    return cpu_cluster_name


def get_dataset(ws, name, date_column):
    ds = Dataset.get_by_name(workspace=ws, name=name)
    ds = ds.with_timestamp_columns(date_column)
    print(ds.to_pandas_dataframe().head(5))
    return ds.register(ws, name, create_new_version=True)


def main(tenant_id, app_id, client_secret):

    # Prepare
    auth = get_auth(tenant_id, app_id, client_secret)
    ws = Workspace.get("avadevitsmlsvc", auth=auth, subscription_id="ff2e23ae-7d7c-4cbd-99b8-116bb94dca6e", resource_group="RG-ITSMLTeam-Dev")
    compute = create_datadrift_compute(ws)

    # Get the same dataset
    baseline_dataset = get_dataset(ws, 'reddit_posts', 'date')
    target_dataset = get_dataset(ws, 'reddit_posts', 'date')

    DDDetectorName = 'reddit_datadrift_detector_1'
    try:
        monitor = DataDriftDetector.get_by_name(ws, DDDetectorName)
    except KeyError:
        # alert_config = AlertConfiguration(['roman.aguilar@accenture.com'])
        monitor = DataDriftDetector.create_from_datasets(
            ws,
            DDDetectorName,
            baseline_dataset,
            target_dataset,
            compute_target=compute,
            drift_threshold=0.2,
            frequency='Month'
        )
        print('Data drift monitor created')


    monitor = DataDriftDetector.get_by_name(ws, DDDetectorName)
    monitor = monitor.update()
    backfill1 = monitor.backfill(datetime(2018, 10, 1), datetime.today())
    print('backfill completed')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validation args")
    parser.add_argument('--tenant_id', dest="tenant_id")
    parser.add_argument('--app_id', dest='app_id')
    parser.add_argument('--client_secret', dest="client_secret")
    args = parser.parse_args()

    main(
        args.tenant_id,
        args.app_id,
        args.client_secret
        )
