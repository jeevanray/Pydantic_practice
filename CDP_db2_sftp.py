import os
import io
from typing import Dict, Any, Optional
from datetime import datetime

import pandas as pd  # ONLY for reading operations
import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pyarrow.parquet as pq

import paramiko

from constants import DATETIMEFORMAT
from minio_helper.audit import prepare_auditing, update_audit_record_strict, initialize_restart_audit_log
from minio_helper.utilities import connect_to_oracle, close_connection, get_oracle_table_schema, \
    create_arrow_table_from_rows, get_system_config , get_job_config
from minio_helper.restartable_logic import get_load_status_and_dates
from minio_helper.logconfig import get_logger

# Use shared logger
logger = get_logger("SFTP_framework")

IST = pytz.timezone("Asia/Kolkata")


def generate_object_path(base_path: str, business_loaddt: str, load_type: str,
                        delta_column_value: Optional[str] = None,
                        sub_folder: Optional[str] = None) -> str:
    """Generate object path based on load type and configuration."""
    try:
        load_dt = datetime.strptime(business_loaddt, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"business_loaddt must be YYYY-MM-DD, got {business_loaddt}") from e

    date_folder = load_dt.strftime("%d%m%Y")

    if load_type == 'historic':
        if delta_column_value:
            clean_delta = str(delta_column_value).replace("-", "").replace(":", "").replace(" ", "")
            return f"{base_path}/history/{clean_delta}"
        raise ValueError("delta_column_value is required for historic load type")
    return f"{base_path}/delta/{date_folder}"


# SFTP helper functions

def init_sftp_client(sftp_config: Dict[str, Any]) -> Dict[str, Any]:
    """Initialize an SFTP connection using paramiko. Returns dict with 'ssh' and 'sftp'.

    Expected sftp_config keys: host, port (optional), username, password (optional), pkey_path (optional)
    """
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    host = sftp_config.get('host')
    port = int(sftp_config.get('port', 22))
    username = sftp_config.get('username')
    password = sftp_config.get('password')
    pkey_path = sftp_config.get('pkey_path')

    try:
        if pkey_path:
            pkey = paramiko.RSAKey.from_private_key_file(pkey_path)
            ssh.connect(hostname=host, port=port, username=username, pkey=pkey, timeout=30)
        else:
            ssh.connect(hostname=host, port=port, username=username, password=password, timeout=30)
        sftp = ssh.open_sftp()
        return {'ssh': ssh, 'sftp': sftp}
    except Exception as e:
        logger.error("Failed to establish SFTP connection to %s:%s - %s", host, port, e)
        raise RuntimeError(f"SFTP connection failed: {e}") from e


def close_sftp_client(client_dict: Dict[str, Any]) -> None:
    try:
        if client_dict is None:
            return
        sftp = client_dict.get('sftp')
        ssh = client_dict.get('ssh')
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass
        if ssh:
            try:
                ssh.close()
            except Exception:
                pass
    except Exception as e:
        logger.warning("Error closing SFTP client: %s", e)


# Upload using PyArrow Table -> parquet -> SFTP
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def upload_arrow_table_parquet_sftp(sftp_client_dict: Dict[str, Any], table: pa.Table,
                                    remote_base_path: str, object_path: str,
                                    compression: str = "snappy") -> None:
    """Write PyArrow Table to Parquet in-memory and upload to SFTP.

    remote_base_path: base directory on the SFTP server
    object_path: relative path (file name) to create under remote_base_path
    """
    sftp = sftp_client_dict.get('sftp')
    if sftp is None:
        raise RuntimeError("SFTP client not initialized")

    remote_full_path = os.path.join(remote_base_path, object_path).replace('\\', '/')
    remote_dir = os.path.dirname(remote_full_path)

    try:
        # Ensure remote directory exists (attempt to create, ignore if exists)
        try:
            # Paramiko SFTP doesn't have mkdir -p; make parent dirs iteratively
            parts = remote_dir.split('/')
            path_builder = ''
            for part in parts:
                if not part:
                    continue
                path_builder = path_builder + '/' + part if path_builder else part
                try:
                    sftp.stat(path_builder)
                except IOError:
                    try:
                        sftp.mkdir(path_builder)
                    except Exception:
                        # may fail due to permissions - ignore and continue
                        pass
        except Exception as e:
            logger.debug("Unable to ensure remote directory structure: %s", e)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=(compression or "snappy"))
        buffer.seek(0)

        # Upload using file-like object; use getbuffer() to avoid extra copy when possible
        with sftp.open(remote_full_path, 'wb') as remote_file:
            mv = buffer.getbuffer()
            # write as memoryview to avoid copying where possible
            remote_file.write(mv)

        logger.debug("Uploaded parquet to SFTP: %s", remote_full_path)

    except Exception as e:
        logger.error("Failed to upload Arrow table to SFTP %s: %s", remote_full_path, e)
        raise RuntimeError(f"Failed to upload Arrow table to SFTP {remote_full_path}: {e}") from e


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def oracle_to_sftp_parquet(
    oracle_config: Dict[str, Any],
    sftp_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    business_loaddt: str,
    *,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    order_by: Optional[str] = None,
    restart_point: int = 0,
    load_type: str = 'delta',
    delta_column: Optional[str] = None,
    delta_column_value: Optional[str] = None,
    sub_folder: Optional[str] = None,
    delta_column_type: Optional[str] = None,
    where_clause: Optional[str] = ""
) -> None:
    """
    Extract data from Oracle and upload as parquet files to SFTP using paramiko.
    """
    if not table_name or not table_name.strip():
        raise ValueError("table_name is required and cannot be empty")
    if not base_object_path:
        raise ValueError("base_object_path is required")
    try:
        logger.info(f"business_loaddt:{business_loaddt}")
        datetime.strptime(business_loaddt, "%Y-%m-%d")
    except ValueError:
        raise ValueError("business_loaddt must be in YYYY-MM-DD format")
    if load_type == 'historic' and not delta_column_value:
        raise ValueError("delta_column_value is required for historic load type")
    if not sftp_config.get("host"):
        raise ValueError("SFTP host is required in configuration")

    extraction_start = datetime.now(IST)
    effective_object_path = generate_object_path(
        base_object_path, business_loaddt, load_type, delta_column_value, sub_folder
    ).replace("//", "/")

    audit_log = prepare_auditing()
    audit_log.update({
        "source_table": table_name.upper(),
        "business_loaddt": business_loaddt,
        "delta_column_value": delta_column_value,
        "load_type": load_type,
        "task_startts": extraction_start.strftime(DATETIMEFORMAT),
        "status": "RUNNING",
        "minio_filepath": effective_object_path,  # kept for compatibility with audit fields
    })

    initialize_restart_audit_log(config_audit, audit_log, business_loaddt, delta_column_value)

    if audit_log.get("status") == "COMPLETED" and audit_log.get("delta_column_value") == delta_column_value:
        logger.info("Job already COMPLETED for %s load_type=%s delta_value=%s. Skipping.",
                   table_name, load_type, delta_column_value)
        return

    # Handle restart logic
    if audit_log.get("status") == "COMPLETED":
        previous_delta = audit_log.get("delta_column_value")
        completed = audit_log.get("aerospike_error") or ""
        audit_log["aerospike_error"] = completed + ("," if completed else "") + str(previous_delta or "")
        previous_total = int(audit_log.get("total_records", 0))
        previous_exec_time = float(audit_log.get("task_exec_secs", 0))

        audit_log.update({
            "delta_column_value": delta_column_value,
            "business_loaddt": delta_column_value,
            "status": "RUNNING",
            "restart_point": 0,
            "task_startts": extraction_start.strftime(DATETIMEFORMAT),
            "total_records": previous_total,
            "task_exec_secs": previous_exec_time,
            "extraction_time": previous_exec_time,
        })
        start_chunk_index = 0
        total_records = previous_total
    else:
        start_chunk_index = max(int(restart_point or 0), int(audit_log.get("restart_point") or 0))
        total_records = int(audit_log.get("total_records") or 0)

    logger.info("Starting extraction for %s on %s (load_type=%s, delta_value=%s)",
               table_name, business_loaddt, load_type, delta_column_value)
    logger.info("Restart chunk index: %s | total_records so far: %s", start_chunk_index, total_records)

    # GET SCHEMA AND TYPE MAP
    try:
        oracle_schema, arrow_type_map = get_oracle_table_schema(oracle_config, table_name)
        logger.info("Retrieved Oracle schema with %d columns for %s", len(oracle_schema), table_name)
    except Exception as e:
        logger.error("Failed to retrieve schema for %s: %s", table_name, e)
        raise RuntimeError(f"Schema retrieval failed: {e}")

    # Build WHERE clause using bind parameters
    where_sql = ""
    sql_params: Dict[str, Any] = {}

    column_names = [f.name for f in oracle_schema]
    column_set = set(column_names)

    if load_type == 'delta':
        if where_clause:
            where_sql = f" WHERE {where_clause}"
    elif load_type == 'historic' and delta_column and delta_column_value:
        if delta_column not in column_set:
            raise ValueError(f"delta_column '{delta_column}' not found in table schema for {table_name}")

        if delta_column_type == 'timestamp':
            if len(delta_column_value) == 10:
                sql_params['delta_start'] = f"{delta_column_value} 00:00:00"
                sql_params['delta_end'] = f"{delta_column_value} 23:59:59"
            else:
                sql_params['delta_start'] = delta_column_value
                sql_params['delta_end'] = delta_column_value

            where_sql = (
                f" WHERE {delta_column} >= TO_TIMESTAMP(:delta_start, 'YYYY-MM-DD HH24:MI:SS')"
                f" AND {delta_column} <= TO_TIMESTAMP(:delta_end, 'YYYY-MM-DD HH24:MI:SS')"
            )
        else:
            sql_params['delta_date'] = delta_column_value
            where_sql = f" WHERE {delta_column} = TO_DATE(:delta_date, 'YYYY-MM-DD')"

    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT * FROM {table_name}{where_sql}{order_clause}"
    logger.info(f"where_sql:{where_sql}, delta_column: {delta_column}, delta_column_value: {delta_column_value} ")

    sftp_client_dict = None
    conn = None
    cur = None
    try:
        conn = connect_to_oracle(oracle_config)
        sftp_client_dict = init_sftp_client(sftp_config)
        sftp = sftp_client_dict.get('sftp')

        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))

        if start_chunk_index and start_chunk_index > 0:
            offset_rows = int(start_chunk_index) * int(chunk_size)
            select_sql = f"{select_sql} OFFSET {offset_rows} ROWS"

        logger.info("Executing SELECT for streaming: %s", select_sql)
        cur.execute(select_sql, sql_params)

        chunk_index = start_chunk_index
        column_names = [field.name for field in oracle_schema]

        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                logger.info("No more data to process for %s", table_name)
                break

            try:
                table_chunk = create_arrow_table_from_rows(rows, column_names, oracle_schema)
                logger.debug("Created Arrow table with %d rows and schema-aligned columns", table_chunk.num_rows)
            except Exception as e:
                logger.error("Failed to create Arrow table from rows: %s", e)
                raise RuntimeError(f"Arrow table creation failed: {e}") from e

            clean_delta = str(delta_column_value).replace("-", "") if delta_column_value else ""
            object_name = (
                f"{effective_object_path}/{table_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
                if load_type == 'historic'
                else f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
            ).replace("//", "/")

            try:
                upload_arrow_table_parquet_sftp(sftp_client_dict, table_chunk, '/', object_name, compression=compression)
                recs = table_chunk.num_rows
                if chunk_index % 10 == 0:
                    logger.info("Successfully uploaded chunk %s (%s rows) to SFTP",
                              chunk_index, recs)
            except Exception as e:
                logger.error("Failed to upload chunk %s to SFTP: %s", chunk_index, e)
                # Attempt to clean up uploaded file if present
                try:
                    remote_path = os.path.join('/', object_name).replace('\\', '/')
                    try:
                        sftp.remove(remote_path)
                    except Exception:
                        pass
                except Exception:
                    pass
                raise RuntimeError(f"SFTP upload failed for chunk {chunk_index}: {e}") from e

            now_ist = datetime.now(IST)
            current_run_recs = table_chunk.num_rows
            previous_total = int(audit_log.get("total_records", 0))
            cumulative_total = previous_total + current_run_recs
            current_run_time = (now_ist - extraction_start).total_seconds()
            previous_exec_time = float(audit_log.get("task_exec_secs", 0))
            cumulative_exec_time = previous_exec_time + float(current_run_time)

            logger.info("PROGRESS: Chunk %d - Current records: %d, Cumulative total: %d records",
                       chunk_index, current_run_recs, cumulative_total)

            audit_log.update({
                "total_records": cumulative_total,
                "status": "RUNNING",
                "task_endts": now_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": cumulative_exec_time,
                "extraction_time": cumulative_exec_time,
                "restart_point": chunk_index + 1,
                "minio_filepath": effective_object_path,
            })

            try:
                update_audit_record_strict(config_audit, audit_log)
                logger.debug("Chunk %s audit update completed", chunk_index)
            except Exception as e:
                logger.error("Audit update failed for chunk %s; deleting uploaded object: %s",
                           chunk_index, object_name)
                try:
                    try:
                        remote_path = os.path.join('/', object_name).replace('\\', '/')
                        sftp.remove(remote_path)
                    except Exception:
                        pass
                    logger.info("Cleaned up uploaded object after audit failure")
                except Exception as del_err:
                    logger.error("Failed to delete object after audit failure: %s", del_err)
                raise RuntimeError(f"Chunk {chunk_index} audit update failed: {e}") from e

            total_records = cumulative_total
            if chunk_index % 10 == 0:
                logger.info("Chunk %s completed (%s rows) -> SFTP/%s", chunk_index, recs, object_name)
            chunk_index += 1

        final_ist = datetime.now(IST)
        current_delta_time = (final_ist - extraction_start).total_seconds()
        previous_exec_time = float(audit_log.get("task_exec_secs", 0))
        cumulative_time = max(previous_exec_time, current_delta_time) if load_type == 'historic' else current_delta_time

        audit_log.update({
            "status": "COMPLETED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": cumulative_time,
            "extraction_time": cumulative_time,
        })

        try:
            update_audit_record_strict(config_audit, audit_log)
            logger.info("Completed Oracle -> SFTP parquet for %s (load_type=%s)",
                       table_name, load_type)
        except Exception as e:
            logger.error("Final audit update failed: %s", e)
            raise RuntimeError(f"Final audit update failed: {e}") from e

    except Exception as e:
        final_ist = datetime.now(IST)
        audit_log.update({
            "status": "FAILED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": (final_ist - extraction_start).total_seconds(),
            "aerospike_error": str(e),
        })
        try:
            update_audit_record_strict(config_audit, audit_log)
        except Exception as audit_err:
            logger.error("Audit update after failure also failed: %s", audit_err)
        logger.error("Oracle -> SFTP transfer failed: %s", e)
        raise RuntimeError(f"Extraction failed: {e}") from e
    finally:
        close_connection(cur, conn)
        if sftp_client_dict:
            close_sftp_client(sftp_client_dict)


def process_oracle_to_sftp_with_dependencies(
    oracle_config: Dict[str, Any],
    sftp_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    current_business_loaddt: str,
    *,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    order_by: Optional[str] = None,
    load_type: str = 'delta',
    delta_column: Optional[str] = None,
    sub_folder: Optional[str] = None,
    delta_column_type: Optional[str] = None,
    delta_column_format: Optional[str] = None,
    where_clause: Optional[str] = ""
) -> None:
    """Sequential processing - processes ALL incomplete jobs in order."""
    logger.info(f"oracle_config: {oracle_config}")
    table_name_uc = table_name.upper()
    logger.info("Begin processing Oracle->SFTP for %s up to %s (load_type=%s)",
               table_name_uc, current_business_loaddt, load_type)


    dates = get_load_status_and_dates(
        config_audit, table_name_uc, current_business_loaddt,
        load_type, delta_column,delta_column_type,delta_column_format, oracle_config=oracle_config
    )

    if not dates:
        logger.info("No jobs to process - all completed or no jobs found.")
        return

    for date_info in dates:
        biz_dt = date_info["business_loaddt"]
        status = date_info["status"]
        restart_point = int(date_info.get("restart_point") or 0)
        delta_value = date_info.get("delta_column_value")
        info_load_type = date_info.get("load_type", load_type)

        logger.info("Processing job %s status=%s restart_point=%s delta_value=%s load_type=%s",
                   biz_dt, status, restart_point, delta_value, info_load_type)

        if status == "COMPLETED" and delta_value == config_audit.get("delta_column_value"):
            logger.info("Job for %s already COMPLETED. Skipping.", biz_dt)
            continue

        if status == "RUNNING":
            logger.warning("Job for %s is RUNNING; treating as FAILED for restart.", biz_dt)
            status = "FAILED"

        if status in ("NOT_STARTED", "FAILED"):
            effective_restart = restart_point if status == "FAILED" else 0

            oracle_to_sftp_parquet(
                oracle_config=oracle_config,
                sftp_config=sftp_config,
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
                where_clause=where_clause
            )

            logger.info("Successfully completed processing for job %s delta_value=%s", biz_dt, delta_value)

    logger.info("All required processing completed for %s", table_name_uc)


def process_from_config(config: Dict[str, Any], conn_config: Dict[str, Any],
                       current_business_loaddt: str) -> None:
    """Process all objects defined in configuration."""

    oracle_config = conn_config.get("target", {})
    sftp_config = conn_config.get("sftp", {})
    logger.info("SFTP config loaded: %s", {k: v for k, v in sftp_config.items() if k != 'password'})

    audit_config = {
        "target": conn_config.get("target", {}),
        "schema": config.get("schema", "appuser"),
        "audit_table": config.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }

    logger.info(f"config:{config.get('objects')}")

    for obj_config in config.get("objects", []):
        logger.info(f"obj_config:{obj_config}")
        if not obj_config.get("isactive", True):
            logger.info("Skipping inactive object: %s", obj_config.get("db_table"))
            continue

        table_name = f"{obj_config.get('schema', config.get('schema'))}.{obj_config['db_table']}"
        output_path = obj_config.get("output_path", config.get("output_path"))
        load_type = obj_config.get("load_type", "delta")
        where_clause = obj_config.get("where_clause", "")

        chunk_size = obj_config.get("chunksize", config.get("chunksize", 100_000))
        order_by = obj_config.get("ORDER_BY")
        delta_column = obj_config.get("delta_column") if load_type == 'historic' else None
        sub_folder = obj_config.get("sub_folder") if load_type == 'historic' else None
        if obj_config.get('schema', config.get('schema',"")).lower() == 'campaign':
            oracle_config = conn_config.get("campaign", {})

        logger.info(f"oracle_config: {oracle_config}")

        if obj_config.get('delta_column_type', config.get('delta_column_type',"")).lower() == 'timestamp':
            delta_column_type = 'timestamp'
            delta_column_format = config.get('delta_column_format',"")
        else:
            delta_column_type = 'date'
            delta_column_format = config.get('delta_column_format',"")


        logger.info("Processing object: %s (load_type=%s)", table_name, load_type)

        try:
            process_oracle_to_sftp_with_dependencies(
                oracle_config=oracle_config,
                sftp_config=sftp_config,
                config_audit=audit_config,
                table_name=table_name,
                base_object_path=output_path,
                current_business_loaddt=current_business_loaddt,
                chunk_size=chunk_size,
                order_by=order_by,
                load_type=load_type,
                delta_column=delta_column,
                sub_folder=sub_folder,
                delta_column_type=delta_column_type,
                delta_column_format=delta_column_format,
                where_clause=where_clause
            )
        except Exception as e:
            logger.error("CRITICAL: Failed to process %s: %s", table_name, e)
            raise RuntimeError(f"Object processing failed for {table_name}: {e}") from e


# Main execution
if __name__ == "__main__":
    get_logger("SFTP_framework")

    try:
        conn_config = get_system_config()
        config_yaml = "etl_configs/db2_to_uds_config.yml"
        config_data = get_job_config(config_yaml)["uds_to_minio"]
        current_date = datetime.now(IST).strftime("%Y-%m-%d")

        logger.info("Starting configuration-based processing with SFTP upload")
        logger.info(f"config_data:{config_data}")
        process_from_config(config_data, conn_config, current_date)
        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error("CRITICAL: Main execution failed: %s", e)
        raise RuntimeError(f"Application failed: {e}") from e
