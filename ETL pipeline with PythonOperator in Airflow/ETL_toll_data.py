### Imports ###

import os
import requests
import tarfile
import csv
import pandas as pd
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


### Defining DAG arguments ###

default_args = {
    'owner': 'jean_ferrer',
    'start_date': days_ago(0), # today
    'email': ['random@etc.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


### Defining the DAG ###

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1), # once, daily
)


### Defining Paths and URLs ###

URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
BASE_DIR = "/home/project/airflow/dags/python_etl"
STAGING_DIR = os.path.join(BASE_DIR, "staging")
DOWNLOAD_PATH = os.path.join(STAGING_DIR, "tolldata.tgz")

# Ensure the staging directory exists
os.makedirs(STAGING_DIR, exist_ok=True)


### Defining Python functions for ETL tasks ###

def download_dataset():
    """
    Task 1: Downloads the dataset from the source URL to the staging directory.
    """
    response = requests.get(URL, stream=True)
    response.raise_for_status() # Raise an exception for bad status codes
    with open(DOWNLOAD_PATH, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Dataset downloaded to {DOWNLOAD_PATH}")

def untar_dataset():
    """
    Task 2: Untars the downloaded .tgz file into the staging directory.
    """
    if not os.path.exists(DOWNLOAD_PATH):
        raise FileNotFoundError(f"{DOWNLOAD_PATH} not found. Please run the download task first.")
    with tarfile.open(DOWNLOAD_PATH, "r:gz") as tar:
        tar.extractall(path=STAGING_DIR)
    print(f"Dataset extracted to {STAGING_DIR}")

def extract_data_from_csv():
    """
    Task 3: Extracts specific columns from vehicle-data.csv.
    The required columns are: Rowid, Timestamp, Anonymized Vehicle number, Vehicle type
    """
    input_file = os.path.join(STAGING_DIR, 'vehicle-data.csv')
    output_file = os.path.join(STAGING_DIR, 'csv_data.csv')
    
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            # Select columns 1, 2, 3, and 4 (0-indexed: 0, 1, 2, 3)
            writer.writerow([row[0], row[1], row[2], row[3]])
    print(f"Extracted data from CSV to {output_file}")

def extract_data_from_tsv():
    """
    Task 4: Extracts specific columns from tollplaza-data.tsv.
    The required columns are: Number of axles, Tollplaza id, Tollplaza code
    """
    input_file = os.path.join(STAGING_DIR, 'tollplaza-data.tsv')
    output_file = os.path.join(STAGING_DIR, 'tsv_data.csv')

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        # Use csv.reader with tab delimiter for TSV files
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile)
        for row in reader:
            # Select columns 5, 6, and 7 (0-indexed: 4, 5, 6)
            writer.writerow([row[4], row[5], row[6]])
    print(f"Extracted data from TSV to {output_file}")

def extract_data_from_fixed_width():
    """
    Task 5: Extracts specific columns from payment-data.txt based on character position.
    The required fields are: Type of Payment code, Vehicle Code
    """
    input_file = os.path.join(STAGING_DIR, 'payment-data.txt')
    output_file = os.path.join(STAGING_DIR, 'fixed_width_data.csv')

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in infile:
            # Extract fields using string slicing (0-indexed)
            # Type of Payment code: positions 59-61
            # Vehicle Code: positions 63-67
            payment_code = line[58:61]
            vehicle_code = line[62:67]
            writer.writerow([payment_code, vehicle_code])
    print(f"Extracted data from fixed-width file to {output_file}")

def consolidate_data():
    """
    Task 6: Combines the three intermediate CSV files into a single file.
    Uses pandas for efficient and reliable column joining.
    """
    csv_file = os.path.join(STAGING_DIR, 'csv_data.csv')
    tsv_file = os.path.join(STAGING_DIR, 'tsv_data.csv')
    fw_file = os.path.join(STAGING_DIR, 'fixed_width_data.csv')
    output_file = os.path.join(STAGING_DIR, 'extracted_data.csv')

    # Read the data files into pandas DataFrames
    df_csv = pd.read_csv(csv_file, header=None)
    df_tsv = pd.read_csv(tsv_file, header=None)
    df_fw = pd.read_csv(fw_file, header=None)

    # Concatenate the DataFrames column-wise
    consolidated_df = pd.concat([df_csv, df_tsv, df_fw], axis=1)
    
    # Save the result to a new CSV file without headers or index
    consolidated_df.to_csv(output_file, index=False, header=False)
    print(f"Consolidated data into {output_file}")

def transform_data():
    """
    Task 7: Transforms the 'Vehicle type' field to uppercase.
    """
    input_file = os.path.join(STAGING_DIR, 'extracted_data.csv')
    output_file = os.path.join(STAGING_DIR, 'transformed_data.csv')
    
    # Read the consolidated data
    df = pd.read_csv(input_file, header=None)
    
    # The 'Vehicle type' is the 4th column (index 3)
    df[3] = df[3].str.upper()
    
    # Save the transformed data
    df.to_csv(output_file, index=False, header=False)
    print(f"Transformed data saved to {output_file}")


### Defining the tasks ###

# Task to download the dataset
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

# Task to untar the dataset
untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)

# Task to extract from vehicle-data.csv
extract_from_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

# Task to extract from tollplaza-data.tsv
extract_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

# Task to extract from payment-data.txt
extract_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

# Task to consolidate the extracted data
consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

# Task to transform the consolidated data
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)


### Task pipeline ###

# Define the sequence of task execution
download_task >> untar_task >> [extract_from_csv_task, extract_from_tsv_task, extract_from_fixed_width_task] >> consolidate_task >> transform_task