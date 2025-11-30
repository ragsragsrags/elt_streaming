from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from datetime import datetime

import os
import json
import pyodbc

path = os.getcwd()

f = open('{0}/dags/replicate_testDB_messages.json'.format(path),)
config = json.load(f)
f.close()

with DAG(
    dag_id='replicate_testDB_messages',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as trigger_dag:
    replicate_testDB_messages = BashOperator(
        task_id='replicate_testDB_messages',
        bash_command=f"python3 /opt/airflow/spark/replicate-testdb-data.py {config["configFile"]}"
    )

    # replicate_testDB_messages

    replicate_testDB_messages_test = BashOperator(
        task_id='replicate_TestDB_messages_test',
        bash_command=f"python3 /opt/airflow/spark/replicate-testdb-data-test.py {config["configFile"]}"
    )

    # replicate_testDB_messages_test

    trigger_child_dag = TriggerDagRunOperator(
        task_id=f'trigger_replicate_testDB_messages',
        trigger_dag_id='replicate_testDB_messages',  # The DAG to trigger
        wait_for_completion=False
    )

    replicate_testDB_messages >> replicate_testDB_messages_test >> trigger_child_dag