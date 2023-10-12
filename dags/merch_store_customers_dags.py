from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta 
from package.merch_store_functions import postgres_db_to_gcs
from package.merch_store_functions import clean_all_customers_df
from package.merch_store_functions import load_from_gcs_to_bigquery
import os

# set environment variableS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/creds/merch-store-399816-963a8ea93cb6.json"

#default dag task
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Instantiate the DAG
dag_customers = DAG(
    'Customers_ETL',
    default_args=default_args,
    description='Extract Transform Load Customers Data To GCP',
    schedule_interval= None,
    tags=['extract_transform_load_GCP_products'],
    catchup=False,
)

#extract data from postgresql to gcs raw  tasks
postgres_to_gcs_raw_customer_task = PythonOperator(
    task_id = 'postgres_to_gcs_raw_customer',
    python_callable = postgres_db_to_gcs,
    op_kwargs = {'your_db_name': 'Merch_Store_Electronics', 'schema': 'electronics', 'table_name': 'dim_customers', 'gcs_bucket_name' : "merch-store-bucket", 'gcs_blob_name':'merch-customers-raw/merch-customers-raw.csv'},
    dag = dag_customers
)

#transform data 
transform_customer_task = PythonOperator(
    task_id = 'transform_customer',
    python_callable = clean_all_customers_df,
    op_kwargs = {'gcs_path_to_raw' :'merch-store-bucket/merch-customers-raw/merch-customers-raw.csv', 'destination_gcs_path' :'merch-store-bucket/merch-customers/merch-customers.parquet'},
    dag = dag_customers
)

#load transformed data to bigquery
load_transformed_customers_bigquery_task = PythonOperator(
    task_id = 'load_transformed_customers_bigquery',
    python_callable = load_from_gcs_to_bigquery,
    op_kwargs = {'gcs_url': 'merch-store-bucket/merch-customers/merch-customers.parquet', 'project_id': 'merch-store-399816', 'destination': 'customers.customers_table'},
    dag = dag_customers
)

#set task dependencies
postgres_to_gcs_raw_customer_task >> transform_customer_task >> load_transformed_customers_bigquery_task

