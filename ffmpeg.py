from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import subprocess
import boto3
import os
from kubernetes.client import models as k8s

# 기본 설정
BUCKET_IN = "privideo-original"
BUCKET_OUT = "privideo-output"
OUTPUT_DIR = "/tmp"
RESOLUTIONS = ["360", "540", "720"]

# 🔥 Airflow Worker + ffmpeg 포함된 커스텀 이미지 사용
WORKER_IMAGE = "leeyonghun/airflow-ffmpeg:v3"

executor_config_transcode = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",               # Airflow Worker 컨테이너는 반드시 base
                    image=WORKER_IMAGE,
                    env_from=[
                        k8s.V1EnvFromSource(
                            secret_ref=k8s.V1SecretEnvSource(name="airflow-aws")
                        )
                    ],
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "1000m", "memory": "2Gi"},
                        limits={"cpu": "2000m", "memory": "4Gi"},
                    ),
                )
            ],
            restart_policy="Never",
        )
    )
}

@task(executor_config=executor_config_transcode)
def transcode_video(dag_run=None, **_):
    """S3에서 동영상을 다운로드 후 해상도별 트랜스코딩"""

    if not dag_run or not dag_run.conf.get("video_id"):
        raise ValueError("❌ video_id parameter is required when triggering the DAG")

    video_id = dag_run.conf.get("video_id")

    s3 = boto3.client("s3")
    input_key = f"org-1/video_{video_id}.mp4"
    local_input = f"{OUTPUT_DIR}/video_{video_id}.mp4"

    print(f"⬇️ Downloading s3://{BUCKET_IN}/{input_key}")
    s3.download_file(BUCKET_IN, input_key, local_input)
    print("✅ Download complete")

    for res in RESOLUTIONS:
        output_local = f"{OUTPUT_DIR}/video_{video_id}_{res}p.mp4"
        print(f"🎬 Transcoding {res}p → {output_local}")

        # ffmpeg 실행
        cmd = (
            f"ffmpeg -y -i {local_input} "
            f"-vf scale=-2:{res} "
            f"-c:v libx264 -preset veryfast -c:a aac {output_local}"
        )
        print(f"🔹 Running command: {cmd}")
        subprocess.run(cmd, shell=True, check=True)

        # S3 업로드
        output_key = f"org-1/video_{video_id}_{res}p.mp4"
        print(f"⬆️ Uploading to s3://{BUCKET_OUT}/{output_key}")
        s3.upload_file(output_local, BUCKET_OUT, output_key)
        print(f"✅ Uploaded {output_key}")

    print("🎉 All resolutions transcoded & uploaded successfully")


with DAG(
    dag_id="trigger_transcode",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ffmpeg", "s3", "k8s", "taskflow"],
) as dag:

    transcode_video()
