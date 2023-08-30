from datetime import datetime

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

from airflow import DAG
from airflow.utils.edgemodifier import Label
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.providers.dbt.cloud.operators.dbt import (DbtCloudRunJobOperator,
                                                       DbtCloudGetJobRunArtifactOperator,
                                                       DbtCloudListJobsOperator
                                                       )
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor


DBT_ACCOUNT_ID =  192555
DBT_CONN_ID = 'arpan_airflow_dbt_conn'

with DAG (
    dag_id='first-dbt-cloud-dag',
    start_date=datetime(2023, 8, 23),
    schedule='@once',
    catchup=False,
    default_args={}
) as dag:

    # trigger_job_run1 = DbtCloudRunJobOperator(
    #         task_id="trigger_job_run1",
    #         job_id=..., # change this
    #         check_interval=10,
    #         timeout=300
    #     )

    get_run_results_artifact = DbtCloudGetJobRunArtifactOperator(
            task_id="get_run_results_artifact", 
            run_id=281338, 
            path="run_results.json"
        )

    # job_run_sensor = DbtCloudJobRunSensor(
    #         task_id="job_run_sensor", 
    #         run_id=trigger_job_run1.output, 
    #         timeout=20
    #     )

    # begin = EmptyOperator(task_id="begin")

    # end = EmptyOperator(task_id="end")