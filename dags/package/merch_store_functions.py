#import libraries
import pandas as pd
import numpy as np
import datetime
import psycopg2
from google.cloud import storage
import gcsfs


#create function to extract data from postgres and load to GCS raw
def postgres_db_to_gcs(your_db_name, schema, table_name, gcs_bucket_name, gcs_blob_name):
    """
    This function loads data from postgresql to GCS.
    Args: 
        your_db_name: Name of source database in postgres
        schema: Name of schema in postgres
        table_name: Name of table in postgres
        gcs_bucket_name: name of bucket in GCS
        gcs_blob_name: link to the destination file in the GCS bucket 
        
        """
    db_params = {
    'host': 'host.docker.internal',
    'dbname': your_db_name,
    'user': 'postgres',
    'password': '####',
    'port': '5432'
    }
    # GCS settings
    #gcs_bucket_name = gcs_bucket_name
    #gcs_blob_name = gcs_blob_name  # Change as needed

    # PostgreSQL query to extract data
    sql_query = f"""SELECT * FROM {schema}.{table_name};"""

    # Connect to PostgreSQL
    #try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Execute the query and fetch data
    cursor.execute(sql_query)
    data = cursor.fetchall()
    
    #get column names
    column_names = [desc[0] for desc in cursor.description]

    # Disconnect from PostgreSQL
    cursor.close()
    conn.close()

    # Write the data to a CSV file
   # csv_content = "\n".join([",".join(map(str, row)) for row in data])

   # Write the column names and data to a CSV file
    csv_content = ",".join(column_names) + "\n" + "\n".join([",".join(map(str, row)) for row in data])


    # Upload the CSV file to GCS
    print(gcs_bucket_name)
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_string(csv_content, content_type="text/csv")

        #print(f"Data has been extracted from PostgreSQL and uploaded to GCS: gs://{gcs_bucket_name}/{gcs_blob_name}")

    #except Exception as e:
        #print(f"An error occurred: {e}")


#create custom functions
def clean_all_products_df(gcs_path_of_raw, destination_gcs_path):
    """
    This function extracts data from source storage on GCS bucket and 
    cleans the product data and saves the resulting file in the destination GCS bucket
    
    Args:
        df: Pandas DataFrame, 
        gcs_path_of_raw: Path of source csv file in google cloud storage bucket,
        destination_gcs_path: Path of destination csv file in google cloud storage bucket,
        column: Column in the Pandas DataFrame
    
    """
    #load data from gcs into dataframe
    #df = pd.read_csv(filepath_or_buffer = gcs_path_of_raw)
    fs = gcsfs.GCSFileSystem(project='merch-store-399816')
    #df = pd.read_csv(gcs_path_to_raw)
    df = pd.read_csv(fs.open(gcs_path_of_raw))
    #remove duplicate rows
    df =df.drop_duplicates()
    #handle missing values
    for column in df.columns:
        if df[column].dtype == 'object': #check if column is non-numeric type
            df[column] = df[column].fillna('not given')
        else: #handle numeric column
            df[column] = df[column].fillna(0)
    #save transformed data to a path on your system
    #df.to_parquet(path = destination_gcs_path, index = False)
    fs = gcsfs.GCSFileSystem(project='merch-store-399816')
    with fs.open(destination_gcs_path, 'wb') as f:
        df.to_parquet(f, index=False)
    
    

def clean_all_purchases_df(gcs_path_of_raw, destination_gcs_path):
    """
    This function cleans the purchase data and saves the resulting file in the destination gcs bucket
    
    Args:
        df: Pandas DataFrame, 
        gcs_path_of_raw: Path of source csv file in google cloud storage bucket,
        destination_gcs_path: Path of destination csv file in google cloud storage bucket,
        column: Column in the Pandas DataFrame
    
    """
    #load data from gcs into dataframe
    #df = pd.read_csv(filepath_or_buffer = gcs_path_of_raw)
    fs = gcsfs.GCSFileSystem(project='merch-store-399816')
    #df = pd.read_csv(gcs_path_to_raw)
    df = pd.read_csv(fs.open(gcs_path_of_raw))

    #remove duplicate rows
    df = df.drop_duplicates()
    #add total amount column
    df['Total_amount'] = df['selling_price']*df['quantity']
    #handle missing values
    for column in df.columns:
        if df[column].dtype == 'object': #check if column is non-numeric type
            df[column] = df[column].fillna('not given')
        else: #handle numeric column
            df[column] = df[column].fillna(0)
     #save transformed data to a path on your system
    #df.to_parquet(path = destination_gcs_path, index = False)
    fs = gcsfs.GCSFileSystem(project='merch-store-399816')
    with fs.open(destination_gcs_path, 'wb') as f:
        df.to_parquet(f, index=False)
    
   

def clean_all_customers_df(gcs_path_to_raw, destination_gcs_path):
    """
    This function cleans the customers data and saves the resulting file in the destination gcs bucket
    Args:
        df: Pandas DataFrame, 
        gcs_path_of_raw: Path of source csv file in google cloud storage bucket,
        destination_gcs_path: Path of destination csv file in google cloud storage bucket,
        column: Column in the Pandas DataFrame
        g : Gender column name in the customers dataframe
    
    """
    #load data from gcs into dataframe
    fs = gcsfs.GCSFileSystem(project='merch-store-399816')
    #df = pd.read_csv(gcs_path_to_raw)
    df = pd.read_csv(fs.open(gcs_path_to_raw))

    snapshot_date = pd.Timestamp(year=2023, month=9, day=20)

    #snapshot_date = datetime.date(year = 2023, month =9 , day= 20)
    #'%Y-%m-%d'
    # Remove duplicate rows
    df = df.drop_duplicates()

    # Clean gender column
    def replace_gender_name(name):
        if isinstance(name, str):
            if name.lower().startswith('m'):
                return 'Male'
            elif name.lower().startswith('f'):
                return 'Female'
            else:
                return 'not given'  # Replace non-alphabet characters with 'not given'
        else:
            return 'not given'  # Handle non-string values
    
    df['gender'] = df['gender'].apply(replace_gender_name)
    df['gender'] = df['gender'].str.title()

    # Add age column
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
    df['Age'] = (snapshot_date - df['date_of_birth']).dt.days // 365

    # Handle missing values
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].fillna('not given')  # Replace missing values in string columns with 'not given'
        elif df[column].dtype == 'int':
            df[column] = df[column].fillna(0)  # Replace missing values in numeric columns with 0

     #save transformed data to a path on your system
    #df.to_parquet(path = destination_gcs_path, index = False)
    # Write to parquet file
    fs = gcsfs.GCSFileSystem(project='merch-store-399816')
    with fs.open(destination_gcs_path, 'wb') as f:
        df.to_parquet(f, index=False)
    
    
#create python function to load data from GCS to BigQuery
def load_from_gcs_to_bigquery(gcs_url, project_id, destination):
    """
    This function loads data from GCS to BigQuery
    Args:
        gcs_url: Source GCS link-> link of storage of cleaned data
        project_id: Project id in GCP
        destination: The id of destination table in BigQuery-> dataset_id.table_id
    """
    #destination = table id
    #read file from gcs
    #df = pd.read_parquet(gcs_url)
    #load data from gcs into dataframe
    fs = gcsfs.GCSFileSystem(project='merch-store-399816')
    #df = pd.read_csv(gcs_path_to_raw)
    df = pd.read_parquet(fs.open(gcs_url))

    #specify destination in bigquery
    bigquery_destination = f"{project_id}.{destination}"
    #Write the Dataframe to BigQuery
    df.to_gbq(bigquery_destination, project_id=project_id, if_exists = 'replace')
