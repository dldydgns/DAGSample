from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os

BUCKET = "privideo-original"
KEY = "test/hello.txt"
LOCAL_FILE = "/tmp/hello.txt"

def upload_hello():
    """S3에 hello.txt 업로드"""
    with open(LOCAL_FILE, "w") as f:
        f.write("hello\n")
    s3 = boto3.client("s3")
    s3.upload_file(LOCAL_FILE, BUCKET, KEY)
    print(f"✅ Uploaded 'hello' to s3://{BUCKET}/{KEY}")

def append_world():
    """S3에서 파일 내려받아 world! 추가 후 다시 업로드"""
    s3 = boto3.client("s3")
    s3.download_file(BUCKET, KEY, LOCAL_FILE)
    with open(LOCAL_FILE, "a") as f:
        f.write("world!\n")
    s3.upload_file(LOCAL_FILE, BUCKET, KEY)
    print(f"✅ Appended 'world!' to s3://{BUCKET}/{KEY}")

with DAG(
    dag_id="s3_hello_world",
    start_date=datetime(2025, 11, 12),
    schedule=None,
    catchup=False,
    tags=["s3", "example"],
) as dag:

    t1 = PythonOperator(
        task_id="upload_hello",
        python_callable=upload_hello,
    )

    t2 = PythonOperator(
        task_id="append_world",
        python_callable=append_world,
    )

    t1 >> t2
