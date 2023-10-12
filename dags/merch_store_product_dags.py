from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta 
from package.merch_store_functions import postgres_db_to_gcs
from package.merch_store_functions import clean_all_products_df
from package.merch_store_functions import load_from_gcs_to_bigquery
import os

# set environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/creds/merch-store-399816-963a8ea93cb6.json"

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

dag_products = DAG(
    'Products_ETL',
    default_args=default_args,
    description='Extract Transform Load Products Data To GCP',
    schedule_interval= None,
    tags=['extract_transform_load_GCP_products'],
    catchup=False,
)

#extract data from postgresql to gcs raw  tasks
postgres_to_gcs_raw_elect_product_task = PythonOperator(
    task_id = 'postgres_to_gcs_raw_elect_product',
    python_callable = postgres_db_to_gcs,
    op_kwargs = {'your_db_name': 'Merch_Store_Electronics', 'schema': 'electronics', 'table_name': 'dim_products', 'gcs_bucket_name' : 'merch-store-bucket', 'gcs_blob_name':'electronics-product-raw/electronics-product-raw.csv' },
    dag = dag_products
)

postgres_to_gcs_raw_fashion_product_task = PythonOperator(
    task_id = 'postgres_to_gcs_raw_fashion_product',
    python_callable = postgres_db_to_gcs,
    op_kwargs = {'your_db_name': 'Merch_Store_Fashion', 'schema': 'fashion', 'table_name': 'dim_products', 'gcs_bucket_name' : 'merch-store-bucket', 'gcs_blob_name':'fashion-product-raw/fashion-product-raw.csv'},
    dag = dag_products
)

#transform data 
transform_elect_product_gcs_raw_task = PythonOperator(
    task_id = 'transform_elect_product_gcs_raw',
    python_callable = clean_all_products_df,
    op_kwargs = {'gcs_path_of_raw' :'merch-store-bucket/electronics-product-raw/electronics-product-raw.csv', 'destination_gcs_path' :'merch-store-bucket/electronics-product/electronics-product.parquet'},
    dag = dag_products
)

transform_fashion_product_gcs_raw_task = PythonOperator(
    task_id = 'transform_fashion_product_gcs_raw',
    python_callable = clean_all_products_df,
    op_kwargs = {'gcs_path_of_raw' :'merch-store-bucket/fashion-product-raw/fashion-product-raw.csv', 'destination_gcs_path' :'merch-store-bucket/fashion-product/fashion-product.parquet'},
    dag = dag_products
)

#load transformed data to bigquery
load_transformed_elect_product_to_bigquery_task = PythonOperator(
    task_id = 'load_transformed_elect_product_to_bigquery',
    python_callable = load_from_gcs_to_bigquery,
    op_kwargs = {'gcs_url': 'merch-store-bucket/electronics-product/electronics-product.parquet', 'project_id': 'merch-store-399816', 'destination': 'electronics_product.electronics_product_table'},
    dag = dag_products
)

load_transformed_fashion_product_to_bigquery_task = PythonOperator(
    task_id = 'load_transformed_fashion_product_to_bigquery',
    python_callable = load_from_gcs_to_bigquery,
    op_kwargs = {'gcs_url': 'merch-store-bucket/fashion-product/fashion-product.parquet', 'project_id': 'merch-store-399816', 'destination': 'fashion_product.fashion_product_table'},
    dag = dag_products
)

#set task dependencies
chain([postgres_to_gcs_raw_elect_product_task, postgres_to_gcs_raw_fashion_product_task], [transform_elect_product_gcs_raw_task, transform_fashion_product_gcs_raw_task], [load_transformed_elect_product_to_bigquery_task, load_transformed_fashion_product_to_bigquery_task] )

