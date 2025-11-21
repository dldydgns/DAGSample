from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import boto3
import os
import subprocess
import shutil
from kubernetes.client import models as k8s

BUCKET_ORIGINAL = "privideo-original"
BUCKET_OUTPUT = "privideo-output"
OUTPUT_DIR = "/workspace"
RESOLUTIONS = ["360", "540", "720"]

def make_container(cpu_req, cpu_limit, mem_req, mem_limit):
    return k8s.V1Container(
        name="base",
        image="leeyonghun/airflow-ffmpeg:v4",
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="airflow-aws"))
        ],
        resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu_req, "memory": mem_req},
            limits={"cpu": cpu_limit, "memory": mem_limit},
        ),
    )

# 모든 트랜스코딩을 하나의 파드에서 처리 (2~3Gi 정도면 충분)
combo_container = make_container("2000m", "3000m", "2Gi", "4Gi")

package_container = make_container("500m", "1000m", "1Gi", "2Gi")

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
    s3 = boto3.client("s3")
    s3_key = f"org-{org_id}/{video_uuid}/original.mp4"
    local_input = f"{OUTPUT_DIR}/{video_uuid}_original.mp4"

    print(f"⬇️ Download → s3://{BUCKET_ORIGINAL}/{s3_key}")
    s3.download_file(BUCKET_ORIGINAL, s3_key, local_input)

    return local_input

# -------------------------------
# 2) 트랜스코딩 (하나의 파드에서 3가지 화질 처리)
# -------------------------------
@task(executor_config=exec_config(combo_container))
def transcode_all_resolutions(local_input: str, org_id: int, video_uuid: str):

    s3 = boto3.client("s3")
    outputs = []

    for res in RESOLUTIONS:
        output_local = f"{OUTPUT_DIR}/{video_uuid}_{res}p.mp4"

        cmd = (
            f"ffmpeg -y -i {local_input} "
            f"-vf scale=-2:{res} "
            f"-c:v libx264 -preset veryfast -c:a aac {output_local}"
        )

        print(f"🎬 Transcoding {res}p → {output_local}")
        subprocess.run(cmd, shell=True, check=True)

        # 업로드
        key = f"org-{org_id}/{video_uuid}/{res}p.mp4"
        print(f"⬆️ Upload {output_local} → s3://{BUCKET_ORIGINAL}/{key}")
        s3.upload_file(output_local, BUCKET_ORIGINAL, key)

        outputs.append(output_local)

    return outputs

# -------------------------------
# 3) 패키징 + 업로드
# -------------------------------
@task(executor_config=exec_config(package_container))
def packaging_and_upload(org_id: int, video_uuid: str, trans_outputs: list):

    s3 = boto3.client("s3")

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

        print(f"📦 Packaging {res}p → {res_dir}")
        subprocess.run(cmd, shell=True, check=True)

        rendition_infos.append((res, f"{res}p/index.m3u8"))

    # MASTER 작성
    master_path = f"{out_dir}/master.m3u8"
    with open(master_path, "w") as m:
        m.write("#EXTM3U\n")
        for res, playlist in rendition_infos:
            bandwidth = int(res) * 1000
            m.write(
                f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION=1920x{res}\n"
                f"{playlist}\n"
            )

    print("⬆️ Uploading all HLS outputs to S3...")

    for root, dirs, files in os.walk(out_dir):
        for file in files:
            local_path = os.path.join(root, file)
            key = f"hls/org-{org_id}/{video_uuid}/{local_path.replace(out_dir, '').lstrip('/')}"
            print(f"S3 → {key}")
            s3.upload_file(local_path, BUCKET_OUTPUT, key)

    print("🎉 Packaging + Upload completed.")
    return True

# -------------------------------
# 4) PVC cleanup
# -------------------------------
@task(executor_config=exec_config(package_container))
def cleanup_local_files(video_uuid: str):

    base = OUTPUT_DIR
    print(f"🧹 Cleaning PVC… {base}")

    for f in os.listdir(base):
        if f.startswith(video_uuid):
            path = os.path.join(base, f)
            print(f"🗑 Removing {path}")
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.remove(path)

    hls_dir = os.path.join(base, f"hls_{video_uuid}")
    if os.path.exists(hls_dir):
        print(f"🗑 Removing HLS: {hls_dir}")
        shutil.rmtree(hls_dir, ignore_errors=True)

    return True

# -------------------------------
# DAG
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

    trans_all = transcode_all_resolutions(original, org_id, video_uuid)

    final = packaging_and_upload(org_id, video_uuid, trans_all)

    clean = cleanup_local_files(video_uuid)

    original >> trans_all >> final >> clean
