it should run the job till it covers all the requirements like if it is historic load then that pod will keep running

Yes—keep the *per-pod* logic exactly as you have today: each pod handles one table/object, and inside that pod it will still run `process_oracle_to_minio_with_dependencies()` so a **historic** object will keep running through all required dates until completed. This still gives parallelism at the Airflow level via dynamic task mapping (`expand_kwargs`) where each object gets its own pod [1].

## DAG code (one pod per object; pod runs full historic/delta requirements)
Use this full DAG (same as before, but the pod args now explicitly call the single-object entrypoint which itself calls `process_oracle_to_minio_with_dependencies`) [1].

```python
import json
import time
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator

from pytz import timezone

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.client import V1PersistentVolumeClaim, V1ObjectMeta
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


DAG_NAME = "CDP_DB_TO_Minio"
CDP_RUN_FILE = "CDP_minio_ingestion.py"

HOME_PATH = Variable.get("HOME_PATH", "/opt/airflow")
SCRIPT_PATH = Variable.get("SCRIPT_PATH")          # e.g. "/opt/airflow/etl_scripts/"
DAGS_PVC = Variable.get("DAGS_PVC")
LOGS_PVC = Variable.get("LOGS_PVC")
STG_PVC = Variable.get("STG_PVC", deserialize_json=True)
IMAGE = Variable.get("IMAGE")
NAMESPACE = Variable.get("NAMESPACE")

CONFIG_YAML_PATH = Variable.get("CONFIG_YAML_PATH", "etl_configs/db2_to_uds_config.yml")
CONFIG_SECTION_KEY = Variable.get("CONFIG_SECTION_KEY", "uds_to_minio")

# Optional: use an Airflow pool to cap concurrent pods (recommended)
K8S_POOL = Variable.get("K8S_POOL", default_var=None)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "HCL MARTECH",
    "email": [],
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": 300,
    "retry_exponential_backoff": True,
    "start_date": datetime(2025, 6, 10),
}


def create_pvc(pvc_dict, namespace=NAMESPACE):
    pvc_name = pvc_dict["name"]
    try:
        counter = 0
        config.load_incluster_config()
        v1_pvc = client.CoreV1Api()

        pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        if pvc is not None:
            logger.info("PVC %s already exists. Status: %s", pvc_name, pvc.status.phase)
            while pvc.status.phase != "Bound":
                time.sleep(5)
                counter += 5
                if counter > 180:
                    raise Exception("Issue Bounding the PVC. Manual intervention required.")
                pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
            logger.info("PVC %s is now: %s", pvc_name, pvc.status.phase)

    except ApiException as e:
        if e.status == 404:
            logger.info("PVC %s not found. Creating!", pvc_name)
            counter = 0
            config.load_incluster_config()
            v1_pvc = client.CoreV1Api()

            pvc = V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name=pvc_name),
                spec={
                    "accessModes": pvc_dict["accessModes"],
                    "resources": pvc_dict["resources"],
                    "storageClassName": pvc_dict["storageClassName"],
                },
            )
            v1_pvc.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)

            while True:
                pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
                phase = getattr(getattr(pvc, "status", None), "phase", None)
                if phase == "Bound":
                    break
                time.sleep(5)
                counter += 5
                if counter > 180:
                    raise Exception("Issue Bounding the PVC. Manual intervention required.")
            logger.info("PVC %s is now: %s", pvc_name, pvc.status.phase)
        else:
            raise


def build_volumes_and_mounts():
    dag_volume = "airflow-dags-volume"
    logs_volume = "airflow-logs-volume"
    stg_volume = "airflow-worker-tmp-volume"

    stg_enabled = str(STG_PVC.get("status", "FALSE")).upper() == "TRUE"
    if stg_enabled:
        create_pvc(STG_PVC)

    def create_volumemount(path, volume_name):
        if path in ("etl_inward_files", "logs"):
            return k8s.V1VolumeMount(name=volume_name, mount_path=f"{HOME_PATH}/{path}")
        return k8s.V1VolumeMount(
            name=volume_name,
            mount_path=f"{HOME_PATH}/{path}",
            sub_path=path.split("_")[-1],
        )

    volume_mounts = []
    for path in ["dags", "etl_scripts", "etl_configs", "etl_metadata"]:
        volume_mounts.append(create_volumemount(path, dag_volume))
    volume_mounts.append(create_volumemount("logs", logs_volume))
    volume_mounts.append(create_volumemount("etl_inward_files", stg_volume))

    volume_dag = k8s.V1Volume(
        name=dag_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=DAGS_PVC),
    )
    volume_log = k8s.V1Volume(
        name=logs_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=LOGS_PVC),
    )
    volume_worker_stg = k8s.V1Volume(
        name=stg_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=STG_PVC["name"]),
    )

    return [volume_dag, volume_log, volume_worker_stg], volume_mounts


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Parallel Oracle->MinIO: one pod per table/object from yaml",
    tags=["cdp", "minio"],
) as dag:

    start_task = DummyOperator(task_id="start")
    end_task = DummyOperator(task_id="end")

    volumes, volume_mounts = build_volumes_and_mounts()

    @task
    def build_pod_kwargs_list() -> list[dict]:
        """
        Produces one mapped KubernetesPodOperator per object/table via dynamic task mapping. [web:1]
        """
        from minio_helper.utilities import get_job_config
        from datetime import datetime
        import pytz

        IST = pytz.timezone("Asia/Kolkata")
        current_date = datetime.now(IST).strftime("%Y-%m-%d")

        cfg = get_job_config(CONFIG_YAML_PATH)[CONFIG_SECTION_KEY]
        objects = cfg.get("objects", [])

        mapped = []
        for obj in objects:
            if not obj.get("isactive", True):
                continue

            obj_json = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

            # IMPORTANT: each pod runs ONE object, but will internally loop dates (historic) as needed
            mapped.append(
                {
                    "task_id": "cdp_minio_ingestion_per_table",
                    "name": f"cdp-minio-{obj.get('db_table','table').lower()}",
                    "cmds": ["bash", "-c"],
                    "arguments": [
                        f"python {SCRIPT_PATH}{CDP_RUN_FILE} "
                        f"--config-yaml '{CONFIG_YAML_PATH}' "
                        f"--section '{CONFIG_SECTION_KEY}' "
                        f"--object-json '{obj_json}' "
                        f"--business-date '{current_date}'"
                    ],
                }
            )
        return mapped

    pod_kwargs = build_pod_kwargs_list()

    per_table_pods = (
        KubernetesPodOperator.partial(
            namespace=NAMESPACE,
            image=IMAGE,
            volumes=volumes,
            volume_mounts=volume_mounts,
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=True,
            pool=K8S_POOL,  # set pool to limit parallel pods if needed
        )
        .expand_kwargs(pod_kwargs)
    )

    start_task >> per_table_pods >> end_task
```

## Python code (single-object entrypoint; still loops historic dates)
This script’s entrypoint runs exactly **one object per pod**, but uses your existing `process_oracle_to_minio_with_dependencies()` so it continues until all required dates are processed (historic/delta restart logic stays inside) [1].

```python
import io
import json
import argparse
from typing import Dict, Any, Optional
from datetime import datetime

import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow.parquet as pq

from constants import DATETIMEFORMAT
from minio_helper.minio_handler import init_minio_client, upload_table, delete_file
from minio_helper.audit import prepare_auditing, update_audit_record_strict, initialize_restart_audit_log
from minio_helper.utilities import (
    connect_to_oracle,
    close_connection,
    get_oracle_table_schema,
    create_arrow_table_from_rows,
    get_system_config,
    get_job_config,
)
from minio_helper.restartable_logic import get_load_status_and_dates
from minio_helper.logconfig import get_logger

logger = get_logger("Minio_framework")
IST = pytz.timezone("Asia/Kolkata")


def generate_object_path(
    base_path: str,
    business_loaddt: str,
    load_type: str,
    delta_column_value: Optional[str] = None,
    sub_folder: Optional[str] = None,
) -> str:
    load_dt = datetime.strptime(business_loaddt, "%Y-%m-%d")
    date_folder = load_dt.strftime("%d%m%Y")

    if load_type == "historic":
        if not delta_column_value:
            raise ValueError("delta_column_value is required for historic load type")
        clean_delta = str(delta_column_value).replace("-", "").replace(":", "").replace(" ", "")
        return f"{base_path}/history/{clean_delta}"

    return f"{base_path}/delta/{date_folder}"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
)
def upload_arrow_table_parquet(minio_client, table, bucket_name: str, object_path, compression: str = "snappy") -> None:
    try:
        try:
            return upload_table(minio_client, table, bucket_name, object_path, compression=compression)
        except Exception:
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression=(compression or "snappy"))
            buffer.seek(0)
            data = buffer.getvalue()
            if hasattr(minio_client, "put_object"):
                minio_client.put_object(
                    bucket_name,
                    object_path,
                    io.BytesIO(data),
                    len(data),
                    content_type="application/octet-stream",
                )
            else:
                raise RuntimeError("Provided minio client does not support put_object")
    except Exception as e:
        logger.error("Failed to upload Arrow table to %s: %s", object_path, e)
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
)
def oracle_to_minio_parquet(
    oracle_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    business_loaddt: str,
    *,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    order_by: Optional[str] = None,
    restart_point: int = 0,
    load_type: str = "delta",
    delta_column: Optional[str] = None,
    delta_column_value: Optional[str] = None,
    sub_folder: Optional[str] = None,
    delta_column_type: Optional[str] = None,
    where_clause: Optional[str] = "",
) -> None:
    datetime.strptime(business_loaddt, "%Y-%m-%d")  # validate

    extraction_start = datetime.now(IST)
    bucket = minio_config["bucket_name"]

    effective_object_path = generate_object_path(
        base_object_path, business_loaddt, load_type, delta_column_value, sub_folder
    ).replace("//", "/")

    audit_log = prepare_auditing()
    audit_log.update(
        {
            "source_table": table_name.upper(),
            "business_loaddt": business_loaddt,
            "delta_column_value": delta_column_value,
            "load_type": load_type,
            "task_startts": extraction_start.strftime(DATETIMEFORMAT),
            "status": "RUNNING",
            "minio_filepath": effective_object_path,
        }
    )

    initialize_restart_audit_log(config_audit, audit_log, business_loaddt, delta_column_value)

    if audit_log.get("status") == "COMPLETED" and audit_log.get("delta_column_value") == delta_column_value:
        logger.info("Job already COMPLETED for %s load_type=%s delta_value=%s. Skipping.",
                    table_name, load_type, delta_column_value)
        return

    if audit_log.get("status") == "COMPLETED":
        previous_delta = audit_log.get("delta_column_value")
        completed = audit_log.get("aerospike_error") or ""
        audit_log["aerospike_error"] = completed + ("," if completed else "") + str(previous_delta or "")
        previous_total = int(audit_log.get("total_records", 0))
        previous_exec_time = float(audit_log.get("task_exec_secs", 0))

        audit_log.update(
            {
                "delta_column_value": delta_column_value,
                "business_loaddt": delta_column_value,
                "status": "RUNNING",
                "restart_point": 0,
                "task_startts": extraction_start.strftime(DATETIMEFORMAT),
                "total_records": previous_total,
                "task_exec_secs": previous_exec_time,
                "extraction_time": previous_exec_time,
            }
        )
        start_chunk_index = 0
        total_records = previous_total
    else:
        start_chunk_index = max(int(restart_point or 0), int(audit_log.get("restart_point") or 0))
        total_records = int(audit_log.get("total_records") or 0)

    oracle_schema, _arrow_type_map = get_oracle_table_schema(oracle_config, table_name)

    # Build where clause exactly like your current logic
    where_part = ""
    if load_type == "delta":
        if where_clause:
            where_part = f" WHERE {where_clause}"
    elif delta_column_type == "timestamp":
        where_part = (
            f" WHERE {delta_column} >= DATE '{delta_column_value}' AND {delta_column} < DATE '{delta_column_value}' + INTERVAL '1' DAY"
            if load_type == "historic"
            else ""
        )
    else:
        where_part = (
            f" WHERE {delta_column} = TO_DATE('{delta_column_value}', 'YYYY-MM-DD')"
            if load_type == "historic" and delta_column and delta_column_value
            else ""
        )

    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT * FROM {table_name}{where_part}{order_clause}"
    logger.info("Executing SELECT: %s", select_sql)

    conn = None
    cur = None
    try:
        conn = connect_to_oracle(oracle_config)
        mclient = init_minio_client(minio_config)
        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))
        cur.execute(select_sql)

        for _ in range(start_chunk_index):
            skipped = cur.fetchmany(chunk_size)
            if not skipped:
                break

        chunk_index = start_chunk_index
        column_names = [field.name for field in oracle_schema]

        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break

            table_chunk = create_arrow_table_from_rows(rows, column_names, oracle_schema)

            clean_delta = str(delta_column_value).replace("-", "") if delta_column_value else ""
            object_name = (
                f"{effective_object_path}/{table_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
                if load_type == "historic"
                else f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
            ).replace("//", "/")

            upload_arrow_table_parquet(mclient, table_chunk, bucket, object_name, compression=compression)

            now_ist = datetime.now(IST)
            current_run_recs = table_chunk.num_rows
            cumulative_total = int(audit_log.get("total_records", 0)) + current_run_recs
            current_run_time = (now_ist - extraction_start).total_seconds()

            audit_log.update(
                {
                    "total_records": cumulative_total,
                    "status": "RUNNING",
                    "task_endts": now_ist.strftime(DATETIMEFORMAT),
                    "task_exec_secs": float(audit_log.get("task_exec_secs", 0)) + float(current_run_time),
                    "extraction_time": float(audit_log.get("task_exec_secs", 0)) + float(current_run_time),
                    "restart_point": chunk_index + 1,
                    "minio_filepath": effective_object_path,
                }
            )

            try:
                update_audit_record_strict(config_audit, audit_log)
            except Exception as e:
                try:
                    delete_file(mclient, bucket, object_name)
                except Exception:
                    if hasattr(mclient, "remove_object"):
                        mclient.remove_object(bucket, object_name)
                raise RuntimeError(f"Chunk {chunk_index} audit update failed: {e}")

            chunk_index += 1

        final_ist = datetime.now(IST)
        audit_log.update(
            {
                "status": "COMPLETED",
                "task_endts": final_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": (final_ist - extraction_start).total_seconds(),
                "extraction_time": (final_ist - extraction_start).total_seconds(),
            }
        )
        update_audit_record_strict(config_audit, audit_log)

    except Exception as e:
        final_ist = datetime.now(IST)
        audit_log.update(
            {
                "status": "FAILED",
                "task_endts": final_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": (final_ist - extraction_start).total_seconds(),
                "aerospike_error": str(e),
            }
        )
        try:
            update_audit_record_strict(config_audit, audit_log)
        except Exception:
            pass
        raise RuntimeError(f"Extraction failed: {e}")
    finally:
        close_connection(cur, conn)


def process_oracle_to_minio_with_dependencies(
    oracle_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    current_business_loaddt: str,
    *,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    order_by: Optional[str] = None,
    load_type: str = "delta",
    delta_column: Optional[str] = None,
    sub_folder: Optional[str] = None,
    delta_column_type: Optional[str] = None,
    delta_column_format: Optional[str] = None,
    where_clause: Optional[str] = "",
) -> None:
    table_name_uc = table_name.upper()

    dates = get_load_status_and_dates(
        config_audit,
        table_name_uc,
        current_business_loaddt,
        load_type,
        delta_column,
        delta_column_type,
        delta_column_format,
        oracle_config=oracle_config,
    )

    if not dates:
        logger.info("No jobs to process - all completed or no jobs found for %s.", table_name_uc)
        return

    for date_info in dates:
        biz_dt = date_info["business_loaddt"]
        status = date_info["status"]
        restart_point = int(date_info.get("restart_point") or 0)
        delta_value = date_info.get("delta_column_value")
        info_load_type = date_info.get("load_type", load_type)

        if status == "RUNNING":
            status = "FAILED"

        if status in ("NOT_STARTED", "FAILED"):
            effective_restart = restart_point if status == "FAILED" else 0
            oracle_to_minio_parquet(
                oracle_config=oracle_config,
                minio_config=minio_config,
                config_audit=config_audit,
                table_name=table_name_uc,
                base_object_path=base_object_path,
                business_loaddt=biz_dt,
                chunk_size=chunk_size,
                compression=compression,
                order_by=order_by,
                restart_point=effective_restart,
                load_type=info_load_type,
                delta_column=delta_column,
                delta_column_value=delta_value,
                sub_folder=sub_folder,
                delta_column_type=delta_column_type,
                where_clause=where_clause,
            )


def run_single_object_from_payload(
    obj_config: Dict[str, Any],
    *,
    config_yaml: str,
    section: str,
    current_business_loaddt: str,
) -> None:
    """
    One pod executes this once; it will still run the FULL requirement set for that table
    because it calls process_oracle_to_minio_with_dependencies() internally.
    """
    conn_config = get_system_config()
    cfg = get_job_config(config_yaml)[section]

    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})

    effective_schema = obj_config.get("schema", cfg.get("schema", "appuser"))
    if effective_schema.lower() == "campaign":
        oracle_config = conn_config.get("campaign", {})

    audit_config = {
        "target": conn_config.get("target", {}),
        "schema": effective_schema,
        "audit_table": cfg.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }

    load_type = obj_config.get("load_type", "delta")
    where_clause = obj_config.get("where_clause", "")

    table_name = f"{effective_schema}.{obj_config['db_table']}"
    output_path = obj_config.get("output_path", cfg.get("output_path"))
    if not output_path:
        raise ValueError(f"output_path not found for object {obj_config}")

    chunk_size = obj_config.get("chunksize", cfg.get("chunksize", 100_000))
    order_by = obj_config.get("ORDER_BY")

    delta_column_type = (obj_config.get("delta_column_type", cfg.get("delta_column_type", "date")) or "date").lower()
    delta_column_format = obj_config.get("delta_column_format", cfg.get("delta_column_format", ""))

    delta_column = obj_config.get("delta_column") if load_type == "historic" else None
    sub_folder = obj_config.get("sub_folder") if load_type == "historic" else None

    process_oracle_to_minio_with_dependencies(
        oracle_config=oracle_config,
        minio_config=minio_config,
        config_audit=audit_config,
        table_name=table_name,
        base_object_path=output_path,
        current_business_loaddt=current_business_loaddt,
        chunk_size=chunk_size,
        order_by=order_by,
        load_type=load_type,
        delta_column=delta_column,
        sub_folder=sub_folder,
        delta_column_type=("timestamp" if delta_column_type == "timestamp" else "date"),
        delta_column_format=delta_column_format,
        where_clause=where_clause,
    )


def parse_args():
    p = argparse.ArgumentParser(description="CDP Oracle -> MinIO (per-table runner)")
    p.add_argument("--config-yaml", required=True, help="Path to job config yaml (mounted inside pod)")
    p.add_argument("--section", required=True, help="Top-level key/section in yaml (e.g. uds_to_minio)")
    p.add_argument("--object-json", required=True, help="JSON string for a single object payload")
    p.add_argument("--business-date", default=None, help="Override business date YYYY-MM-DD (optional)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    obj = json.loads(args.object_json)
    business_date = args.business_date or datetime.now(IST).strftime("%Y-%m-%d")

    logger.info("Starting per-object pod run db_table=%s load_type=%s business_date=%s",
                obj.get("db_table"), obj.get("load_type"), business_date)

    run_single_object_from_payload(
        obj_config=obj,
        config_yaml=args.config_yaml,
        section=args.section,
        current_business_loaddt=business_date,
    )

    logger.info("Completed per-object pod run db_table=%s", obj.get("db_table"))
```

## One operational reminder
To actually *see* pods run in parallel, make sure you don’t accidentally serialize them with Airflow limits; dynamic task mapping will create the tasks, but concurrency is still governed by Airflow settings/pools (`pool` is the simplest lever) [1].

If the YAML can contain hundreds of tables, what maximum number of parallel pods is safe for your Oracle + MinIO (so the pool size can be set accordingly)?

