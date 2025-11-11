from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

FILE_PATH = "/tmp/hello.txt"

def write_hello():
    with open(FILE_PATH, "w") as f:
        f.write("hello")
    print(f"✅ Created {FILE_PATH}")

def append_world():
    with open(FILE_PATH, "a") as f:
        f.write(" world!")
    print(f"✅ Appended ' world!' to {FILE_PATH}")

with DAG(
    dag_id="simple_file_write",
    start_date=datetime(2025, 11, 5),
    schedule=None,  # ✅ Airflow 3.x syntax
    catchup=False,
    tags=["example"],
) as dag:
    create = PythonOperator(
        task_id="create_file",
        python_callable=write_hello,
    )

    append = PythonOperator(
        task_id="append_file",
        python_callable=append_world,
    )

    create >> append
