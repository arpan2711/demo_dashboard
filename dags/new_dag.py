from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = "arpan-airflow-sf-conn"
SNOWFLAKE_TABLE_FOR_QUERY = "EMP_BASIC"

SQL_DROP_TABLE_SPEND_PER_CUSTOMER_IF_EXISTS = ( 
    f"DROP TABLE IF EXISTS {SNOWFLAKE_TABLE_FOR_QUERY}_airflow;" 
)

SQL_CREATE_TABLE_SPEND_PER_CUSTOMER = ( 
    f"CREATE TABLE {SNOWFLAKE_TABLE_FOR_QUERY}_airflow AS SELECT * FROM {SNOWFLAKE_TABLE_FOR_QUERY} LIMIT 5;" 
)

with DAG (
    dag_id='first-snowflake-dag',
    start_date=datetime(2023, 8, 23),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    schedule_interval='@once',
    catchup=False
) as dag:
    
    
    # first task
    drop_table_spend_per_customer_if_exists=SnowflakeOperator(
        task_id='drop_table_spend_per_customer_if_exists',
        sql=SQL_CREATE_TABLE_SPEND_PER_CUSTOMER
    )

    # # second task
    # clone_table_spend_per_customer=SnowflakeOperator(
    #     task_id='clone_table_spend_per_customer',
    #     sql=SQL_CREATE_TABLE_SPEND_PER_CUSTOMER
    # )

   