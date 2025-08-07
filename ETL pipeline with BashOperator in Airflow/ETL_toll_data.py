### Imports ###

from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
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


### Defining the tasks ###

# Task 1: Unzip data
# Unzips the 'tolldata.tgz' file.
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)

# Task 2: Extract data from CSV
# Extracts required columns and removes carriage returns to prevent blank lines.
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
        tr -d '\\r' < /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv | \
        awk -F, 'BEGIN {OFS=","} {print $1, $2, $3, $4}' > \
        /home/project/airflow/dags/finalassignment/staging/csv_data.csv
    """,
    dag=dag,
)

# Task 3: Extract data from TSV
# Extracts required columns using a tab delimiter and removes carriage returns.
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
        tr -d '\\r' < /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv | \
        awk -F$'\\t' 'BEGIN {OFS=","} {print $5, $6, $7}' > \
        /home/project/airflow/dags/finalassignment/staging/tsv_data.csv
    """,
    dag=dag,
)

# Task 4: Extract data from fixed-width file
# Extracts fields by character position and removes carriage returns.
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
        tr -d '\\r' < /home/project/airflow/dags/finalassignment/staging/payment-data.txt | \
        awk 'BEGIN {OFS=","} {print substr($0, 59, 3), substr($0, 63, 5)}' > \
        /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv
    """,
    dag=dag,
)

# Task 5: Consolidate data
# Merges the three clean intermediate CSV files into a single file.
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," /home/project/airflow/dags/finalassignment/staging/csv_data.csv \
                               /home/project/airflow/dags/finalassignment/staging/tsv_data.csv \
                               /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv \
                               > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
    dag=dag,
)

# Task 6: Transform data
# Converts the fourth field (Vehicle type) to uppercase.
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F, \'BEGIN {OFS=","} {$4=toupper($4); print}\' \
                  /home/project/airflow/dags/finalassignment/staging/extracted_data.csv \
                  > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)


### Task pipeline ###

unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data