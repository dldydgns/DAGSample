from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import boto3
import os
import subprocess
from kubernetes.client import models as k8s

BUCKET_ORIGINAL = "privideo-original"
BUCKET_OUTPUT = "privideo-output"
OUTPUT_DIR = "/tmp"
RESOLUTIONS = ["360", "540", "720"]

# ------------------------------------------------
# 공통 컨테이너 (트랜스코딩 용 - 고사양)
# ------------------------------------------------
transcode_container = k8s.V1Container(
    name="base",
    image="leeyonghun/airflow-ffmpeg:v4",
    env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="airflow-aws"))],
    resources=k8s.V1ResourceRequirements(
        requests={"cpu": "2000m", "memory": "3Gi"},
        limits={"cpu": "4000m", "memory": "6Gi"},
    ),
)

# ------------------------------------------------
# 패키징 전용 (저사양)
# ------------------------------------------------
package_container = k8s.V1Container(
    name="base",
    image="leeyonghun/airflow-ffmpeg:v4",
    env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="airflow-aws"))],
    resources=k8s.V1ResourceRequirements(
        requests={"cpu": "500m", "memory": "1Gi"},
        limits={"cpu": "1000m", "memory": "2Gi"},
    ),
)

def exec_config(container):
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[container],
                restart_policy="Never",
            )
        )
    }


# ------------------------------------------------
# 1) 다운로드
# ------------------------------------------------
@task(executor_config=exec_config(package_container))
def download_video(org_id: int, video_uuid: str):

    s3 = boto3.client("s3")
    s3_key = f"org-{org_id}/{video_uuid}/original.mp4"
    local_input = f"{OUTPUT_DIR}/{video_uuid}_original.mp4"

    print(f"⬇️ Download → s3://{BUCKET_ORIGINAL}/{s3_key}")
    s3.download_file(BUCKET_ORIGINAL, s3_key, local_input)

    return local_input


# ------------------------------------------------
# 2) 트랜스코딩 (병렬)
# ------------------------------------------------
@task(executor_config=exec_config(transcode_container))
def transcode_video(local_input: str, res: str, org_id: int, video_uuid: str):

    output_local = f"{OUTPUT_DIR}/{video_uuid}_{res}p.mp4"

    cmd = (
        f"ffmpeg -y -i {local_input} "
        f"-vf scale=-2:{res} "
        f"-c:v libx264 -preset veryfast -c:a aac {output_local}"
    )

    print(f"🎬 Transcoding {res}p → {output_local}")
    subprocess.run(cmd, shell=True, check=True)

    # S3 업로드 (트랜스코딩 mp4 → original bucket)
    s3 = boto3.client("s3")
    key = f"org-{org_id}/{video_uuid}/{res}p.mp4"

    print(f"⬆️ Upload {output_local} → s3://{BUCKET_ORIGINAL}/{key}")
    s3.upload_file(output_local, BUCKET_ORIGINAL, key)

    return output_local


# ------------------------------------------------
# 3) 패키징 + 업로드 (한 번에 처리)
# ------------------------------------------------
@task(executor_config=exec_config(package_container))
def packaging_and_upload(org_id: int, video_uuid: str, trans_outputs: list):

    s3 = boto3.client("s3")

    out_dir = f"{OUTPUT_DIR}/hls_{video_uuid}"
    os.makedirs(out_dir, exist_ok=True)

    rendition_infos = []

    # 개별 해상도 HLS
    for mp4_path in trans_outputs:
        res = mp4_path.split("_")[-1].replace("p.mp4", "")
        res_dir = f"{out_dir}/{res}p"
        os.makedirs(res_dir, exist_ok=True)

        cmd = (
            f"ffmpeg -i {mp4_path} -c copy "
            f"-map 0 -f hls -hls_time 10 -hls_playlist_type vod "
            f"-hls_segment_filename '{res_dir}/segment%03d.ts' "
            f"{res_dir}/index.m3u8"
        )

        print(f"📦 Packaging {res}p → {res_dir}")
        subprocess.run(cmd, shell=True, check=True)

        rendition_infos.append((res, f"{res}p/index.m3u8"))

    # MASTER 생성
    master_path = f"{out_dir}/master.m3u8"
    with open(master_path, "w") as m:
        m.write("#EXTM3U\n")
        for res, playlist in rendition_infos:
            bandwidth = int(res) * 1000
            m.write(
                f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION=1920x{res}\n"
                f"{playlist}\n"
            )

    # ---- 전체 업로드 ----
    print("⬆️ Uploading HLS package to S3...")

    for root, dirs, files in os.walk(out_dir):
        for file in files:
            local_path = os.path.join(root, file)
            key = f"org-{org_id}/{video_uuid}/{local_path.replace(out_dir, '').lstrip('/')}"
            print(f"S3 → {key}")
            s3.upload_file(local_path, BUCKET_OUTPUT, key)

    print("🎉 All packaging + upload finished")
    return True


# ------------------------------------------------
# DAG
# ------------------------------------------------
with DAG(
    dag_id="video_transcode_hls_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    org_id = "{{ dag_run.conf['org_id'] }}"
    video_uuid = "{{ dag_run.conf['video_uuid'] }}"

    original = download_video(org_id, video_uuid)

    trans_tasks = [
        transcode_video(original, r, org_id, video_uuid)
        for r in RESOLUTIONS
    ]

    final = packaging_and_upload(org_id, video_uuid, trans_tasks)

    original >> trans_tasks >> final
