from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

IMAGE_NAME = "dockdock150/ffmpeg-airflow:latest"
BUCKET = "privideo-original"

INPUT_KEY = "input/sample.mp4"             # 원본 영상
TEMP_INPUT = "temp/input.mp4"              # 중간 저장용
TEMP_OUTPUT = "temp/output.mp4"            # 중간 저장용
FINAL_OUTPUT = "output/sample_720p.mp4"    # 최종 결과물

AWS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY"
AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_KEY"

with DAG(
    dag_id="ffmpeg_transcode_steps",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ffmpeg", "s3", "k8s"],
):

    # 1️⃣ S3 → temp/input.mp4 다운로드 & S3 업로드
    download = KubernetesPodOperator(
        task_id="download_video",
        name="download-video",
        namespace="airflow",
        image=IMAGE_NAME,
        image_pull_policy="IfNotPresent",
        cmds=["bash", "-c"],
        arguments=[f"""
            echo ">>> 영상 다운로드"
            aws s3 cp s3://{BUCKET}/{INPUT_KEY} /tmp/input.mp4
            
            echo ">>> temp 영역에 업로드"
            aws s3 cp /tmp/input.mp4 s3://{BUCKET}/{TEMP_INPUT}
        """],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # 2️⃣ FFmpeg 트랜스코딩
    transcode = KubernetesPodOperator(
        task_id="transcode_video",
        name="transcode-video",
        namespace="airflow",
        image=IMAGE_NAME,
        image_pull_policy="IfNotPresent",
        cmds=["bash", "-c"],
        arguments=[f"""
            echo ">>> temp/input.mp4 다운로드"
            aws s3 cp s3://{BUCKET}/{TEMP_INPUT} /tmp/input.mp4

            echo ">>> FFmpeg 트랜스코딩"
            ffmpeg -i /tmp/input.mp4 -vf scale=1280:720 -b:v 3000k /tmp/output.mp4 -y

            echo ">>> temp/output.mp4 업로드"
            aws s3 cp /tmp/output.mp4 s3://{BUCKET}/{TEMP_OUTPUT}
        """],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # 3️⃣ temp/output.mp4 → 최종 output 폴더로 업로드
    upload = KubernetesPodOperator(
        task_id="upload_transcoded_video",
        name="upload-video",
        namespace="airflow",
        image=IMAGE_NAME,
        image_pull_policy="IfNotPresent",
        cmds=["bash", "-c"],
        arguments=[f"""
            echo ">>> temp/output.mp4 다운로드"
            aws s3 cp s3://{BUCKET}/{TEMP_OUTPUT} /tmp/output.mp4

            echo ">>> 최종 output 폴더에 업로드"
            aws s3 cp /tmp/output.mp4 s3://{BUCKET}/{FINAL_OUTPUT}

            echo ">>> 완료!"
        """],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    download >> transcode >> upload
