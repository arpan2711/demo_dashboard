from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import os


#import kaggle


# -------python functions
def download_data():
    # Create the directory (if it doesn't exist)
    os.makedirs("/home/airflow/.kaggle", exist_ok=True)
    

    with open("/home/airflow/.kaggle/kaggle.json", "w") as file:
        file.write('{"username":"arpanmitra222","key":"76579f2b456157e4fb6bc3298d632980"}')

    # os.makedirs('./kaggle', exist_ok=True)

    import kaggle

    kaggle.api.authenticate()
    # kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce', './kaggle' , unzip=True)
    cmd = "kaggle datasets download -d olistbr/brazilian-ecommerce"
    result = os.system(cmd)


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(dag_id='python-dag', 
         start_date=datetime(2023, 8, 23), 
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    

    # making call to the api via function
    kaggle_data = PythonOperator(
        task_id='make_new_folder',
        python_callable=download_data
    )



