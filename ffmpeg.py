from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import boto3
import os
import subprocess
import shutil
import time
import json
import re
from kubernetes.client import models as k8s


# -------------------------------
# 기본 설정
# -------------------------------
BUCKET_ORIGINAL = "privideo-original"
BUCKET_OUTPUT = "privideo-output"
OUTPUT_DIR = "/workspace"
RESOLUTIONS = ["360", "540", "720"]


# -------------------------------
# MediaConvert 스타일 로그 함수
# -------------------------------
def mc_log(event, job_id, detail=None, level="INFO", start_time=None):
    log = {
        "timestamp": int(time.time() * 1000),
        "level": level,
        "event": event,
        "jobId": job_id,
        "detail": detail or {}
    }

    if start_time is not None:
        log["durationMs"] = int((time.time() - start_time) * 1000)

    print(json.dumps(log))


# -------------------------------
# ffmpeg 진행률 추출 (원하면 사용)
# -------------------------------
def parse_progress(stderr_text):
    match = re.search(r"time=([0-9:.]+)", stderr_text)
    return match.group(1) if match else None


# -------------------------------
# 리소스 컨테이너 생성
# -------------------------------
def make_container(cpu_req, cpu_limit, mem_req, mem_limit):
    return k8s.V1Container(
        name="base",
        image="leeyonghun/airflow-ffmpeg:v4",
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="airflow-aws")
            )
        ],
        resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu_req, "memory": mem_req},
            limits={"cpu": cpu_limit, "memory": mem_limit},
        ),
    )


def get_transcode_container(res):
    if res == "360":
        return make_container("1000m", "1000m", "1Gi", "2Gi")
    elif res == "540":
        return make_container("1000m", "2000m", "1Gi", "3Gi")
    elif res == "720":
        return make_container("1000m", "3000m", "1Gi", "4Gi")
    else:
        raise ValueError("Unsupported resolution")


package_container = make_container("500m", "1000m", "1Gi", "2Gi")


# -------------------------------
# PVC + CloudWatch logging Pod Override
# -------------------------------
def exec_config(container):
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                tolerations=[
                    k8s.V1Toleration(
                        key="role",
                        operator="Equal",
                        value="airflow-worker",
                        effect="NoSchedule"
                    )
                ],
                node_selector={"role": "airflow-worker"},
                containers=[
                    k8s.V1Container(
                        name=container.name,
                        image=container.image,
                        env_from=container.env_from,
                        resources=container.resources,
                        volume_mounts=[
                            k8s.V1VolumeMount(
                                name="worker-temp",
                                mount_path="/workspace"
                            )
                        ],
                        # -----------------------------
                        # CloudWatch Logs 설정
                        # -----------------------------
                        log_configuration=k8s.V1LogConfiguration(
                            log_driver="awslogs",
                            options={
                                "awslogs-region": "ap-northeast-2",
                                "awslogs-group": "/airflow/video-transcode",
                                "awslogs-stream-prefix": "task",
                                "awslogs-create-group": "true",
                            }
                        ),
                    )
                ],
                volumes=[
                    k8s.V1Volume(
                        name="worker-temp",
                        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                            claim_name="pvc-hdd-airflow-worker-temp"
                        )
                    )
                ],
                restart_policy="Never",
            )
        )
    }


# -------------------------------
# 1) 다운로드
# -------------------------------
@task(executor_config=exec_config(package_container))
def download_video(org_id: int, video_uuid: str):

    start = time.time()
    job_id = f"{org_id}-{video_uuid}"

    s3 = boto3.client("s3")
    key = f"org-{org_id}/{video_uuid}/original.mp4"
    local_path = f"{OUTPUT_DIR}/{video_uuid}_original.mp4"

    s3.download_file(BUCKET_ORIGINAL, key, local_path)

    mc_log(
        event="DOWNLOAD",
        job_id=job_id,
        start_time=start,
        detail={
            "input": f"s3://{BUCKET_ORIGINAL}/{key}",
            "output": local_path
        }
    )

    return local_path


# -------------------------------
# 2) 트랜스코딩 (해상도별)
# -------------------------------
def build_transcode_task(resolution):

    container = get_transcode_container(resolution)

    @task(
        task_id=f"transcode_video_{resolution}p",
        executor_config=exec_config(container)
    )
    def _transcode(local_input: str, org_id: int, video_uuid: str):

        start = time.time()
        job_id = f"{org_id}-{video_uuid}"

        output_local = f"{OUTPUT_DIR}/{video_uuid}_{resolution}p.mp4"

        cmd = (
            f"ffmpeg -y -i {local_input} "
            f"-vf scale=-2:{resolution} "
            f"-c:v libx264 -preset veryfast -c:a aac {output_local}"
        )

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            mc_log(
                event="TRANSCODE_ERROR",
                job_id=job_id,
                level="ERROR",
                start_time=start,
                detail={
                    "resolution": resolution,
                    "stderr": result.stderr,
                }
            )
            raise Exception(result.stderr)

        s3 = boto3.client("s3")
        key = f"org-{org_id}/{video_uuid}/{resolution}p.mp4"
        s3.upload_file(output_local, BUCKET_ORIGINAL, key)

        mc_log(
            event="TRANSCODE",
            job_id=job_id,
            start_time=start,
            detail={
                "resolution": resolution,
                "output": key
            }
        )

        return output_local

    return _transcode


# -------------------------------
# 3) 패키징 + 업로드
# -------------------------------
@task(executor_config=exec_config(package_container))
def packaging_and_upload(org_id: int, video_uuid: str, trans_outputs: list):

    start = time.time()
    job_id = f"{org_id}-{video_uuid}"

    out_dir = f"{OUTPUT_DIR}/hls_{video_uuid}"
    os.makedirs(out_dir, exist_ok=True)

    rendition_infos = []

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

        subprocess.run(cmd, shell=True, check=True)
        rendition_infos.append((res, f"{res}p/index.m3u8"))

    # Master playlist
    master_path = f"{out_dir}/master.m3u8"
    with open(master_path, "w") as m:
        m.write("#EXTM3U\n")
        for res, playlist in rendition_infos:
            bandwidth = int(res) * 1000
            m.write(
                f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION=1920x{res}\n"
                f"{playlist}\n"
            )

    s3 = boto3.client("s3")

    # segment + playlist upload
    for root, dirs, files in os.walk(out_dir):
        for file in files:
            if file == "master.m3u8":
                continue
            local_path = os.path.join(root, file)
            key = f"hls/org-{org_id}/{video_uuid}/{local_path.replace(out_dir, '').lstrip('/')}"
            s3.upload_file(local_path, BUCKET_OUTPUT, key)

    final_master_key = f"hls/org-{org_id}/{video_uuid}/master.m3u8"
    s3.upload_file(master_path, BUCKET_OUTPUT, final_master_key)

    mc_log(
        event="PACKAGING",
        job_id=job_id,
        start_time=start,
        detail={
            "hlsPrefix": f"hls/org-{org_id}/{video_uuid}/"
        }
    )

    return True


# -------------------------------
# 4) PVC 정리
# -------------------------------
@task(executor_config=exec_config(package_container))
def cleanup_local_files(video_uuid: str):

    start = time.time()
    job_id = f"cleanup-{video_uuid}"

    base = OUTPUT_DIR

    for f in os.listdir(base):
        if f.startswith(video_uuid):
            path = os.path.join(base, f)
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.remove(path)

    hls_dir = os.path.join(base, f"hls_{video_uuid}")
    if os.path.exists(hls_dir):
        shutil.rmtree(hls_dir, ignore_errors=True)

    mc_log(event="CLEANUP", job_id=job_id, start_time=start)

    return True


# -------------------------------
# DAG 정의
# -------------------------------
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
        build_transcode_task(r)(original, org_id, video_uuid)
        for r in RESOLUTIONS
    ]

    final = packaging_and_upload(org_id, video_uuid, trans_tasks)

    clean = cleanup_local_files(video_uuid)

    original >> trans_tasks >> final >> clean
