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
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import os
import pandas as pd

# python functions ------------------------------

def download_data():
    # Create the directory (if it doesn't exist)
    os.makedirs("/home/airflow/.kaggle", exist_ok=True)
    

    with open("/home/airflow/.kaggle/kaggle.json", "w") as file:
        file.write('{"username":"arpanmitra222","key":"76579f2b456157e4fb6bc3298d632980"}')

    # os.makedirs('./kaggle', exist_ok=True)

    import kaggle

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce', '/home/airflow/kaggle_data' , unzip=True)
    # direc = '/home/airflow/.kaggle/kaggle_data'
    # files = os.listdir(direc)
    
    # # Filter out only the .csv files
    # csv_files = [f for f in files if f.endswith('.csv')]
    
    # for csv_file in csv_files:
    #     # Read the CSV file
    #     df = pd.read_csv(os.path.join(direc, csv_file))
        
    #     # Convert the dataframe to JSON
    #     json_file = csv_file.replace('.csv', '.json')
    #     df.to_json(os.path.join(direc, json_file))
    #     # Delete the CSV file
    #     os.remove(os.path.join(direc, csv_file))

# snowflake stuff ------------------------------

SNOWFLAKE_CONN_ID = "arpan-airflow-sf-conn"
# SNOWFLAKE_TABLE_FOR_QUERY = "EMP_BASIC"

SQL_CREATE_TABLE_SPEND_PER_CUSTOMER = ( 
    f"put file:///home/airflow/kaggle_data/olist_customers_dataset.csv @SF_TUTS.PUBLIC.%olist_customers_dataset;" 
)

SQL_LOAD_DATA = (
    f"copy into olist_customers_dataset from @%olist_customers_dataset file_format = (format_name = 'my_csv_format' , error_on_column_count_mismatch=false) pattern = '.*olist_customers_dataset.csv.gz' on_error = 'skip_file';"
)

SQL_LOAD_DATA2 = (
    f"put file:///home/airflow/kaggle_data/product_category_name_translation.csv @SF_TUTS.PUBLIC.%product_category_name_translation;" 

)

SQL_LOAD_DATA25 = (
    f"copy into product_category_name_translation from @%product_category_name_translation file_format = (format_name = 'my_csv_format' , error_on_column_count_mismatch=false) pattern = '.*product_category_name_translation.csv.gz' on_error = 'skip_file';"
)


# dbt stuff ------------------------------










# airflow dags ------------------------------

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "snowflake_conn_id": SNOWFLAKE_CONN_ID
}

with DAG(dag_id='one-dag', 
         start_date=datetime(2023, 8, 23), 
         schedule_interval='@once',
         default_args=default_args,
         catchup=False) as dag:
    
    begin = EmptyOperator(task_id="begin")

    # making call
    kaggle_data = PythonOperator(
        task_id='make_new_folder',
        python_callable=download_data
    )


    # exporting to snowflake 
    drop_table_spend_per_customer_if_exists=SnowflakeOperator(
        task_id='drop_table_spend_per_customer_if_exists',
        sql=SQL_CREATE_TABLE_SPEND_PER_CUSTOMER
    )


    fill_table=SnowflakeOperator(
        task_id='fill_table',
        sql=SQL_LOAD_DATA
    )

    second_table=SnowflakeOperator(
        task_id='sec_table',
        sql=SQL_LOAD_DATA2
    )

    fill_table2=SnowflakeOperator(
        task_id='fill_table2',
        sql=SQL_LOAD_DATA25
    )

begin >> kaggle_data

 
