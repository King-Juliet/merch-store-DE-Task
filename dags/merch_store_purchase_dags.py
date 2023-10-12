from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
from packages.merch_store_functions import postgres_db_to_gcs
from packages.merch_store_functions import clean_all_purchases_df
from packages.merch_store_functions import load_from_gcs_to_bigquery
import os

# set environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/creds/merch-store-399816-963a8ea93cb6.json"

#define DAGs and tasks

##default dag task
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
dag_purchases = DAG(
    'Purchases_ETL',
    default_args=default_args,
    description='Extract Transform Load Purchases Data To GCP',
    schedule_interval= None,
    tags=['extract_transform_load_GCP_purchases'],
    catchup=False,
)

#extract data from postgresql to gcs raw  tasks
postgres_to_gcs_raw_elect_purchase_task = PythonOperator(
    task_id = 'postgres_to_gcs_raw_elect_purchase',
    python_callable = postgres_db_to_gcs,
    op_kwargs = {'your_db_name': 'Merch_Store_Electronics', 'schema': 'electronics', 'table_name': 'fct_orders', 'gcs_bucket_name' : 'merch-store-bucket', 'gcs_blob_name':'electronics-purchase-raw/electronics-purchase-raw.csv'},
    dag = dag_purchases
)

postgres_to_gcs_raw_fashion_purchase_task = PythonOperator(
    task_id = 'postgres_to_gcs_raw_fashion_purchase',
    python_callable = postgres_db_to_gcs,
    op_kwargs = {'your_db_name': 'Merch_Store_Fashion', 'schema': 'fashion', 'table_name': 'fct_orders', 'gcs_bucket_name' : 'merch-store-bucket', 'gcs_blob_name':'fashion-purchase-raw/fashion-purchase-raw.csv'},
    dag = dag_purchases
)

#transform data 
transform_elect_purchase_gcs_raw_task = PythonOperator(
    task_id = 'transform_elect_purchase_gcs_raw',
    python_callable = clean_all_purchases_df,
    op_kwargs = {'gcs_path_of_raw' :'merch-store-bucket/electronics-purchase-raw/electronics-purchase-raw.csv', 'destination_gcs_path' :'merch-store-bucket/electronics-purchase/electronics-purchase.parquet'},
    dag = dag_purchases
)

transform_fashion_purchase_gcs_raw_task = PythonOperator(
    task_id = 'transform_fashion_purchase_gcs_raw',
    python_callable = clean_all_purchases_df,
    op_kwargs = {'gcs_path_of_raw' :'merch-store-bucket/fashion-purchase-raw/fashion-purchase-raw.csv', 'destination_gcs_path' :'merch-store-bucket/fashion-purchase/fashion-purchase.parquet'},
    dag = dag_purchases
)

#load transformed data to bigquery
load_transformed_elect_purchase_to_bigquery_task = PythonOperator(
    task_id = 'load_transformed_elect_purchase_to_bigquery',
    python_callable = load_from_gcs_to_bigquery,
    op_kwargs = {'gcs_url': 'merch-store-bucket/electronics-purchase/electronics-purchase.parquet', 'project_id': 'merch-store-399816', 'destination': 'electronics_purchase.electronics_purchase_table'},
    dag = dag_purchases
)

load_transformed_fashion_purchase_to_bigquery_task = PythonOperator(
    task_id = 'load_transformed_fashion_purchase_to_bigquery',
    python_callable = load_from_gcs_to_bigquery,
    op_kwargs = {'gcs_url': 'merch-store-bucket/fashion-purchase/fashion-purchase.parquet', 'project_id': 'merch-store-399816', 'destination': 'fasion_purchase.fashion_purchase_table'},
    dag = dag_purchases
)

#set task dependencies
chain([postgres_to_gcs_raw_elect_purchase_task, postgres_to_gcs_raw_fashion_purchase_task], [transform_elect_purchase_gcs_raw_task, transform_fashion_purchase_gcs_raw_task], [load_transformed_elect_purchase_to_bigquery_task, load_transformed_fashion_purchase_to_bigquery_task] )

