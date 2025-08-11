# IBM Apache Airflow & Apache Kafka

These are the final assignments of IBM's "ETL and Data Pipelines with Shell, Airflow and Kafka" course.  
Link for the course: https://www.coursera.org/learn/etl-and-data-pipelines-shell-airflow-kafka

This repository contains three distinct projects that demonstrate building data pipelines using different tools and methodologies: two batch ETL pipelines orchestrated with Apache Airflow and one real-time streaming ETL pipeline using Apache Kafka.

---

## 📂 Project 1: ETL Pipeline with BashOperator in Airflow

This project implements a complete ETL (Extract, Transform, Load) process using Apache Airflow, where each task is executed as a shell command via the `BashOperator`. The pipeline processes toll data from multiple sources, cleans it, and prepares it for analysis.

### Pipeline Logic

The DAG (`ETL_toll_data.py`) defines a series of tasks to process toll data:

1.  **Unzip Data**: The initial task unzips the compressed `tolldata.tgz` file into a staging directory.
2.  **Extract Data (Parallel Tasks)**: Three parallel tasks extract relevant columns from different file types by specifying the correct field separator for each:
    * `extract_data_from_csv`: Pulls specific columns from `vehicle-data.csv`, a **C**omma-**S**eparated **V**alues file. The `awk` command uses its default comma (`,`) delimiter to parse the fields.
    * `extract_data_from_tsv`: Pulls specific columns from `tollplaza-data.tsv`, a **T**ab-**S**eparated **V**alues file. Here, the `awk` command is explicitly configured with `-F$'\\t'` to use the tab character as a delimiter.
    * `extract_data_from_fixed_width`: Extracts data from `payment-data.txt` based on fixed character positions, as this format does not use delimiters.
3.  **Consolidate Data**: This task merges the three intermediate CSV files into a single file, `extracted_data.csv`.
4.  **Transform Data**: The final task reads the consolidated data and transforms the `Vehicle type` column to uppercase, saving the result as `transformed_data.csv`.

### Task Pipeline

The dependencies between the tasks are defined as follows, allowing for parallel execution of the extraction steps.

```python
unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data
````

### How to Run

1.  Create the staging directory: `sudo mkdir -p /home/project/airflow/dags/finalassignment/staging`
2.  Set permissions: `sudo chmod -R 777 /home/project/airflow/dags/finalassignment`
3.  Download the raw data: `sudo curl https://.../tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz`
4.  Copy the DAG file to your Airflow DAGs folder and enable it in the Airflow UI.

-----

## 📂 Project 2: ETL Pipeline with PythonOperator in Airflow

This project accomplishes the same ETL goal as the first one but utilizes Airflow's `PythonOperator`. Each task is defined by a dedicated Python function, offering greater flexibility, maintainability, and easier testing compared to using `BashOperator`. It leverages popular Python libraries like **Pandas**, **requests**, and the native **csv** module.

### Pipeline Logic

The DAG (`ETL_toll_data.py`) is refactored to use Python callables for each step:

1.  **Download & Untar**: The first two tasks handle downloading the dataset from a URL and extracting it.
2.  **Extract Data (Parallel Tasks)**: Three Python functions leverage the csv module to handle different flat-file formats:
    * `extract_data_from_csv`: Processes `vehicle-data.csv` by reading it with the csv module, which uses a comma (`,`) as the default delimiter.
    * `extract_data_from_tsv`: Processes `tollplaza-data.tsv`. While structurally similar to a CSV, this function specifically configures the csv.reader with `delimiter='\t'` to correctly parse the tab-separated fields.
    * `extract_data_from_fixed_width`: Parses `payment-data.txt` by slicing strings based on fixed character positions, as it lacks delimiters.
4.  **Consolidate Data**: A function using `pandas.concat` reads the three intermediate files into DataFrames and joins them column-wise, which is more robust than the shell `paste` command.
5.  **Transform Data**: The final transformation step also uses **Pandas** to load the consolidated data, apply the `.str.upper()` method to the `Vehicle type` column, and save the final output.

### Task Pipeline

The task dependency graph remains logically the same, but now includes the download and untar steps as part of the DAG.

```python
download_task >> untar_task >> [extract_from_csv_task, extract_from_tsv_task, extract_from_fixed_width_task] >> consolidate_task >> transform_task
```

### How to Run

1.  Create the staging directory: `sudo mkdir -p /home/project/airflow/dags/python_etl/staging`
2.  Set permissions: `sudo chmod -R 777 /home/project/airflow/dags/python_etl`
3.  Copy the DAG file to your Airflow DAGs folder and enable it in the Airflow UI. The pipeline will handle data download automatically.

-----

## 📂 Project 3: Streaming ETL Pipeline in Kafka

This project demonstrates a real-time data processing pipeline using **Apache Kafka** and **MySQL**. It simulates a continuous stream of toll traffic data, which is consumed, processed, and stored in a database in real-time.

### Architecture

The pipeline consists of three main components:

1.  **Producer (`toll_traffic_generator.py`)**: This Python script simulates a live traffic feed. It continuously generates random toll data (vehicle ID, type, plaza ID) and publishes it as messages to a Kafka topic named `toll`.
2.  **Apache Kafka**: Acts as the distributed, fault-tolerant message broker that decouples the producer from the consumer. It buffers the incoming stream of data.
3.  **Consumer (`streaming-data-reader.py`)**: This script connects to the Kafka cluster and subscribes to the `toll` topic. It reads messages as they arrive, performs a light transformation on the timestamp format, and inserts the data into a `livetolldata` table in a **MySQL** database.

### Data Flow

`toll_traffic_generator.py` → **Kafka Topic (`toll`)** → `streaming-data-reader.py` → **MySQL Database**

### How to Run

1.  **Setup Kafka**: Download, extract, and start a Kafka server using KRaft.
2.  **Setup MySQL**: Connect to a MySQL server, create a `tolldata` database, and create the `livetolldata` table with the specified schema.
3.  **Install Dependencies**: Install the required Python libraries: `pip install kafka-python mysql-connector-python`.
4.  **Create Kafka Topic**: Create the `toll` topic in your Kafka cluster.
5.  **Run Producer**: Execute `python3 toll_traffic_generator.py` to start generating and sending data.
6.  **Run Consumer**: In a separate terminal, execute `python3 streaming-data-reader.py` to start consuming data and loading it into MySQL.
7.  **Verify**: Connect to MySQL and query the `livetolldata` table to see the incoming data.
