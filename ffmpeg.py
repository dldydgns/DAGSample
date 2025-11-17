from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import boto3
import os

# S3 버킷 이름
BUCKET_IN = "privideo-original"
BUCKET_OUT = "privideo-output"
OUTPUT_DIR = "/tmp"
RESOLUTIONS = ["360", "540", "720"]


def transcode_video(**context):
    """video_id를 받아 S3에서 다운로드 후 다중 해상도 트랜스코딩"""
    video_id = context["dag_run"].conf.get("video_id")
    if not video_id:
        raise ValueError("❌ video_id parameter is required when triggering the DAG")

    s3 = boto3.client("s3")
    input_key = f"org-1/video_{video_id}.mp4"
    local_input = f"{OUTPUT_DIR}/video_{video_id}.mp4"

    print(f"⬇️ Downloading s3://{BUCKET_IN}/{input_key}")
    s3.download_file(BUCKET_IN, input_key, local_input)
    print("✅ Download complete")

    for res in RESOLUTIONS:
        output_local = f"{OUTPUT_DIR}/video_{video_id}_{res}p.mp4"
        print(f"🎬 Transcoding {res}p → {output_local}")
        cmd = [
            "ffmpeg", "-y",
            "-i", local_input,
            "-vf", f"scale=-2:{res}",
            "-c:v", "libx264",
            "-preset", "medium",
            "-c:a", "aac",
            output_local
        ]
        subprocess.run(cmd, check=True)

        output_key = f"org-1/video_{video_id}_{res}p.mp4"
        print(f"⬆️ Uploading to s3://{BUCKET_OUT}/{output_key}")
        s3.upload_file(output_local, BUCKET_OUT, output_key)
        print(f"✅ Uploaded {output_key}")

    print("🎉 All resolutions transcoded & uploaded successfully")


with DAG(
    dag_id="trigger_transcode",
    start_date=datetime(2025, 11, 18),
    schedule=None,
    catchup=False,
    tags=["ffmpeg", "s3", "k8s", "dynamic"],
) as dag:

    transcode = PythonOperator(
        task_id="transcode_with_params",
        python_callable=transcode_video,
        provide_context=True,
        executor_config={
            "KubernetesExecutor": {
                "image": "jrottenberg/ffmpeg:6.0-ubuntu",
                "resources": {"request_cpu": "1000m", "request_memory": "2Gi"},
                "envFrom": [{"secretRef": {"name": "airflow-aws"}}],
            }
        },
    )
