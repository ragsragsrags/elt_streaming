from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime

import os
import json
import pyodbc

path = os.getcwd()

f = open('{0}/dags/start_insert_testDB_messages_streaming.json'.format(path),)
config = json.load(f)
f.close()

destination = config["destination"]
preloadTables = config["preloadTables"]
pyodbc_url = destination["pyodbc_url"]

print("destination: ", destination)
print("preloadTables: ", preloadTables)

def get_preload_status():
    is_initialized = False
    is_preloaded = False
    is_preloaded_validated = False
    tables = []

    for table in preloadTables:
        tables.append(table["name"])

    tablesCsv = ",".join(tables)
    print("tablesCsv: ", tablesCsv)

    conn = pyodbc.connect(pyodbc_url)
    cursor = conn.cursor()
    
    cursor.execute(f"EXEC [dbo].[GetPreloadStatus] '{tablesCsv}'")

    for row in cursor:
        is_initialized = bool(row.is_initialized)
        is_preloaded = bool(row.is_preloaded)
        is_preloaded_validated = bool(row.is_preloaded_validated)

    print("is_initialized: ", is_initialized)
    print("is_preloaded: ", is_preloaded)
    print("is_preloaded_validated: ", is_preloaded_validated)

    cursor.close()
    conn.close()

    return is_initialized, is_preloaded, is_preloaded_validated

def get_initialize_testdb_data():
    return BashOperator(
        task_id='initialize_testdb',
        bash_command=f"spark-submit --master {config["sparkMaster"]} --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 --jars {config["sparkJars"]} /opt/airflow/spark/initialize-testdb-data.py {config["configFile"]} "
    )

def get_preload_testdb_data():
    return BashOperator(
        task_id='preload_testDB',
        bash_command=f"python3 /opt/airflow/spark/preload-testdb-data.py {config["configFile"]}"
    )

def get_preload_testdb_data_test():
    return BashOperator(
        task_id='preload_testDB_test',
        bash_command=f"python3 /opt/airflow/spark/preload-testdb-data-test.py {config["configFile"]}"
    )

def get_start_insert_testDB_messages_streaming():
    return BashOperator(
        task_id='start_insert_testDB_messages_streaming',
        bash_command=f"spark-submit --master {config["sparkMaster"]} --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 --jars {config["sparkJars"]} /opt/airflow/spark/insert-testdb-messages.py {config["configFile"]} "
    )

with DAG(
    dag_id='start_insert_testDB_messages_streaming',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    is_initialized, is_preloaded, is_preloaded_validated = get_preload_status()
     
    if is_preloaded_validated == True:
        get_start_insert_testDB_messages_streaming()
    elif is_preloaded == True:
        get_preload_testdb_data_test() >> get_start_insert_testDB_messages_streaming()
    elif is_initialized == True:
        get_preload_testdb_data() >> get_preload_testdb_data_test() >> get_start_insert_testDB_messages_streaming()
    else:
        get_initialize_testdb_data() >> get_preload_testdb_data() >> get_preload_testdb_data_test() >> get_start_insert_testDB_messages_streaming()
    
    # get_preload_testdb_data()
    # get_preload_testdb_data_test()