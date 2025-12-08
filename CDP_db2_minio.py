import os
import io
from typing import Dict, Any, Optional
from datetime import datetime
 
import pandas as pd  # ONLY for reading operations
import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pyarrow.parquet as pq
 
# from cdp_diapi_adapter import get_system_config
from constants import DATETIMEFORMAT
from minio_helper.minio_handler import init_minio_client, upload_table, delete_file
from minio_helper.audit import prepare_auditing, update_audit_record_strict, initialize_restart_audit_log
from minio_helper.utilities import connect_to_oracle, close_connection, get_oracle_table_schema, \
    create_arrow_table_from_rows, get_system_config , get_job_config
from minio_helper.restartable_logic import get_load_status_and_dates
from minio_helper.logconfig import get_logger
 
# Use shared logger
logger = get_logger("Minio_framework")
 
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
 
 
# NEW: Pure PyArrow upload - NO pandas
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def upload_arrow_table_parquet(minio_client, table, bucket_name: str,
                               object_path, compression: str = "snappy") -> None:
    """
    Upload PyArrow Table directly as parquet. NO pandas involved.
    """
    try:
        try:
            return upload_table(minio_client, table, bucket_name, object_path, compression=compression)
        except Exception:
            # Fallback: write to buffer and use put_object directly without duplicating memory
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression=(compression or "snappy"))
            buffer.seek(0)
            size = buffer.getbuffer().nbytes
            if hasattr(minio_client, "put_object"):
                # pass buffer directly to avoid copying the bytes
                minio_client.put_object(bucket_name, object_path, buffer, size, content_type="application/octet-stream")
            else:
                raise RuntimeError("Provided minio client does not support put_object")

    except Exception as e:
        logger.error(f"Failed to upload Arrow table to {object_path}: {e}")
        raise RuntimeError(f"Failed to upload Arrow table to {object_path}: {e}") from e
 
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
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
    load_type: str = 'delta',
    delta_column: Optional[str] = None,
    delta_column_value: Optional[str] = None,
    sub_folder: Optional[str] = None,
    delta_column_type: Optional[str] = None,
    where_clause: Optional[str] = ""
) -> None:
    """
    Extract data from Oracle to MinIO parquet using pure PyArrow.
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
    if not minio_config.get("bucket_name"):
        raise ValueError("MinIO bucket_name is required in configuration")
 
    extraction_start = datetime.now(IST)
    bucket = minio_config["bucket_name"]
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
        "minio_filepath": effective_object_path,
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
 
    # GET SCHEMA AND TYPE MAP (your requested change)
    try:
        oracle_schema, arrow_type_map = get_oracle_table_schema(oracle_config, table_name)
        logger.info("Retrieved Oracle schema with %d columns for %s", len(oracle_schema), table_name)
    except Exception as e:
        logger.error("Failed to retrieve schema for %s: %s", table_name, e)
        raise RuntimeError(f"Schema retrieval failed: {e}")
    # Build WHERE clause using bind parameters to avoid SQL injection and to handle date/timestamp safely
    where_sql = ""
    sql_params: Dict[str, Any] = {}

    # Validate schema-derived column names
    column_names = [f.name for f in oracle_schema]
    column_set = set(column_names)

    if load_type == 'delta':
        if where_clause:
            # where_clause provided in config -- assume trusted, include as-is
            where_sql = f" WHERE {where_clause}"
    elif load_type == 'historic' and delta_column and delta_column_value:
        # Ensure delta column exists in table schema
        if delta_column not in column_set:
            raise ValueError(f"delta_column '{delta_column}' not found in table schema for {table_name}")

        if delta_column_type == 'timestamp':
            # Expect delta_column_value to be 'YYYY-MM-DD' or full timestamp string
            # Build start and end of day bounds for the given date
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
            # Treat as DATE
            sql_params['delta_date'] = delta_column_value
            where_sql = f" WHERE {delta_column} = TO_DATE(:delta_date, 'YYYY-MM-DD')"

    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT * FROM {table_name}{where_sql}{order_clause}"
    logger.info(f"where_sql:{where_sql}, delta_column: {delta_column}, delta_column_value: {delta_column_value} ")
 
    conn = None
    cur = None
    try:
        conn = connect_to_oracle(oracle_config)
        mclient = init_minio_client(minio_config)
        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))

        # If resuming from a restart_point (measured as chunk index), prefer using SQL OFFSET
        # to avoid iterating rows in Python. OFFSET is computed as restart_index * chunk_size
        if start_chunk_index and start_chunk_index > 0:
            offset_rows = int(start_chunk_index) * int(chunk_size)
            select_sql = f"{select_sql} OFFSET {offset_rows} ROWS"

        logger.info("Executing SELECT for streaming: %s", select_sql)
        # Use binds where applicable
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
                raise RuntimeError(f"Arrow table creation failed: {e}")
 
            clean_delta = str(delta_column_value).replace("-", "") if delta_column_value else ""
            object_name = (
                f"{effective_object_path}/{table_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
                if load_type == 'historic'
                else f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
            ).replace("//", "/")
 
            try:
                upload_arrow_table_parquet(mclient, table_chunk, bucket, object_name, compression=compression)
                recs = table_chunk.num_rows
                if chunk_index % 10 == 0:
                    logger.info("Successfully uploaded chunk %s (%s rows) with Arrow-optimized schema",
                              chunk_index, recs)
            except Exception as e:
                logger.error("Failed to upload chunk %s to MinIO: %s", chunk_index, e)
                raise RuntimeError(f"MinIO upload failed for chunk {chunk_index}: {e}")
 
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
            # logger.info(f'audit_log:{audit_log}')
 
            try:
                update_audit_record_strict(config_audit, audit_log)
                logger.debug("Chunk %s audit update completed", chunk_index)
            except Exception as e:
                logger.error("Audit update failed for chunk %s; deleting uploaded object: %s/%s",
                           chunk_index, bucket, object_name)
                try:
                    try:
                        delete_file(mclient, bucket, object_name)
                    except Exception:
                        # fallback to Minio client's remove_object
                        if hasattr(mclient, "remove_object"):
                            mclient.remove_object(bucket, object_name)
                    logger.info("Cleaned up uploaded object after audit failure")
                except Exception as del_err:
                    logger.error("Failed to delete object after audit failure: %s", del_err)
                raise RuntimeError(f"Chunk {chunk_index} audit update failed: {e}")
 
            total_records = cumulative_total
            if chunk_index % 10 == 0:
                logger.info("Chunk %s completed (%s rows) -> %s/%s", chunk_index, recs, bucket, object_name)
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
            logger.info("Completed Oracle -> MinIO parquet for %s (load_type=%s) with pure PyArrow optimization",
                       table_name, load_type)
        except Exception as e:
            logger.error("Final audit update failed: %s", e)
            raise RuntimeError(f"Final audit update failed: {e}")
 
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
        logger.error("Oracle -> MinIO transfer failed: %s", e)
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
    logger.info("Begin processing Oracle->MinIO for %s up to %s (load_type=%s)",
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
                where_clause=where_clause
            )
 
            logger.info("Successfully completed processing for job %s delta_value=%s", biz_dt, delta_value)
 
    logger.info("All required processing completed for %s", table_name_uc)
 
def process_from_config(config: Dict[str, Any], conn_config: Dict[str, Any],
                       current_business_loaddt: str) -> None:
    """Process all objects defined in configuration."""
 
    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})
    logger.info("MinIO config loaded: %s", {k: v for k, v in minio_config.items() if k != 'secret_key'})
 
    audit_config = {
        "target": conn_config.get("target", {}),
        "schema": config.get("schema", "appuser"),
        "audit_table": config.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }
 
    logger.info(f"config:{config.get("objects")}")
 
    for obj_config in config.get("objects", []):
        logger.info(f"obj_config:{obj_config}")
        if not obj_config.get("isactive", True):
            logger.info("Skipping inactive object: %s", obj_config.get("db_table"))
            continue
 
        table_name = f"{obj_config.get('schema', config.get('schema'))}.{obj_config['db_table']}"
        output_path = obj_config.get("output_path", config.get("output_path"))
        load_type = obj_config.get("load_type", "delta")
        where_clause = obj_config.get("where_clause", "")
        # if load_type == 'delta':
        #     if where_clause:
 
 
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
                delta_column_type=delta_column_type,
                delta_column_format=delta_column_format,
                where_clause=where_clause
            )
        except Exception as e:
            logger.error("CRITICAL: Failed to process %s: %s", table_name, e)
            raise RuntimeError(f"Object processing failed for {table_name}: {e}")
 
 
# Main execution
if __name__ == "__main__":
    get_logger("Minio_framework")
 
    try:
        conn_config = get_system_config()
        config_yaml = "etl_configs/db2_to_uds_config.yml"
        config_data = get_job_config(config_yaml)["uds_to_minio"]
        current_date = datetime.now(IST).strftime("%Y-%m-%d")
 
        logger.info("Starting configuration-based processing with pure PyArrow optimization")
        logger.info(f"config_data:{config_data}")
        process_from_config(config_data, conn_config, current_date)
        logger.info("Processing completed successfully")
 
    except Exception as e:
        logger.error("CRITICAL: Main execution failed: %s", e)
        raise RuntimeError(f"Application failed: {e}")
