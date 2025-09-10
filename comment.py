import os
import io
import logging
import time
from typing import Dict, Any, List, Optional, Union, Sequence

import pandas as pd
import oracledb
from oracledb import LOB  # Moved import to top (MJ #9)
import pytz
from datetime import datetime
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa  # Moved import to top (MJ #23)
import pyarrow.parquet as pq

# External dependencies
from minio_handler import MinioHandler
from cdp_diapi_adapter import get_system_config, close_connection  ## MJ - TODO: Confirm if close_connection exists in cdp_diapi_adapter; if not, define it here
from Constants import DATETIMEFORMAT, ORACLE_DATE_FMT  ## MJ - TODO: Confirm Constants.py exists with DATETIMEFORMAT="%Y-%m-%d %H:%M:%S" and ORACLE_DATE_FMT="YYYY-MM-DD"; if not, define it

# Logging
logger = logging.getLogger("DIAPI")  # Changed to DIAPI (MJ #1)

# TZ
IST = pytz.timezone("Asia/Kolkata")

# Configuration Helper
def load_config_from_yaml(yaml_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# DB Helpers
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def connect_to_oracle(oracle_conf: Dict[str, Any]) -> oracledb.Connection:
    """Connect to Oracle database with retry logic and MANDATORY error raising."""
    logger.info("Connecting to Oracle DB")  # Changed to info (MJ #4)
    try:
        conn = oracledb.connect(
            user=oracle_conf.get("username", oracle_conf.get("user", "")),
            password=oracle_conf["password"],
            dsn=oracle_conf["dsn"],
        )
        logger.info("Oracle connection successful")
        return conn
    except Exception as e:
        logger.error("CRITICAL: Failed to connect to Oracle database: %s", e)
        raise RuntimeError(f"Oracle connection failed: {e}")  # Simplified error handling (MJ #5)

# Audit Helpers
def prepare_auditing(source_table: str = "", load_type: str = "delta", 
                    business_loaddt: str = "") -> Dict[str, Any]:  # Added parameters (MJ #7)
    """Base audit log dictionary with all expected keys and optional initial values."""
    return {
        "source_table": source_table,
        "task_startts": "",
        "task_endts": "",
        "task_exec_secs": 0,
        "business_loaddt": business_loaddt,
        "delta_column_value": None,
        "total_records": 0,
        "extraction_time": 0,
        "total_apicalls": 0,
        "success_apicalls": 0,
        "failed_apicalls": 0,
        "api_failedpath": None,
        "apicall_time": 0,
        "cdp_db_count_validation": False,  # Indicates if CDP DB count validation was performed (MJ #11)
        "aerospike_init_record_cnt": 0,
        "aerospike_init_read_waittime": 0,
        "aerospike_record_cnt": 0,
        "aerospike_waittime": 0,
        "aerospike_error": None,
        "mongodb_init_record_cnt": 0,
        "mongodb_record_cnt": 0,
        "mongodb_init_read_waittime": 0,
        "mongodb_waittime": 0,
        "mongodb_error": None,
        "suspected_updates_or_blacklisted_records": 0,
        "difference_aero_mongo": 0,
        "status": "",
        "log_path": "",
        "minio_filepath": "",
        "restart_point": 0,
        "load_type": load_type,
        "created_at_ts": None,
        "updated_at_ts": None,
    }

def _parse_datetime(value: Optional[str], format: str, is_date: bool = False) -> Optional[Union[datetime, datetime.date]]:
    """Helper to parse datetime or date strings (MJ #13)."""
    if isinstance(value, str) and value:
        parsed = datetime.strptime(value, format)
        return parsed.date() if is_date else IST.localize(parsed)
    return None

def _ensure_time_fields(audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize types for time fields and booleans prior to MERGE. Modifies in place (MJ #14)."""
    # Configurable datetime fields (MJ #12)
    datetime_fields = ["task_startts", "task_endts"]
    date_fields = ["business_loaddt"]

    audit_data["cdp_db_count_validation"] = 'Y' if audit_data.get("cdp_db_count_validation") else 'N'

    for field in datetime_fields:
        audit_data[field] = _parse_datetime(audit_data.get(field), DATETIMEFORMAT)

    for field in date_fields:
        audit_data[field] = _parse_datetime(audit_data.get(field), "%Y-%m-%d", is_date=True)

    now_ist = datetime.now(IST)
    audit_data["updated_at_ts"] = now_ist
    if not audit_data.get("created_at_ts"):
        audit_data["created_at_ts"] = now_ist
    return audit_data

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def initialize_restart_audit_log(config_audit: Dict[str, Any], audit_log: Dict[str, Any], aud_dt: str,
                                delta_column_value: Optional[str] = None) -> None:
    """
    Load existing audit record for restart. Historic loads use a single record per source_table,
    while delta loads use separate records per business_loaddt (MJ #8).
    """
    if audit_log.get("load_type") == "historic":
        query = f"""
            SELECT
                source_table, task_startts, task_endts, task_exec_secs, business_loaddt,
                delta_column_value, total_records, extraction_time, total_apicalls,
                success_apicalls, failed_apicalls, api_failedpath, apicall_time,
                cdp_db_count_validation, aerospike_init_record_cnt, aerospike_init_read_waittime,
                aerospike_record_cnt, aerospike_waittime, aerospike_error,
                mongodb_init_record_cnt, mongodb_record_cnt, mongodb_init_read_waittime,
                mongodb_waittime, mongodb_error, suspected_updates_or_blacklisted_records,
                difference_aero_mongo, status, log_path, minio_filepath, restart_point,
                load_type
            FROM {config_audit["schema"]}.{config_audit["audit_table"]}
            WHERE source_table = :src
            AND load_type = 'historic'
            ORDER BY updated_at_ts DESC NULLS LAST
            FETCH FIRST 1 ROW ONLY
        """
        params = {"src": audit_log["source_table"]}
    else:
        query = f"""
            SELECT
                source_table, task_startts, task_endts, task_exec_secs, business_loaddt,
                delta_column_value, total_records, extraction_time, total_apicalls,
                success_apicalls, failed_apicalls, api_failedpath, apicall_time,
                cdp_db_count_validation, aerospike_init_record_cnt, aerospike_init_read_waittime,
                aerospike_record_cnt, aerospike_waittime, aerospike_error,
                mongodb_init_record_cnt, mongodb_record_cnt, mongodb_init_read_waittime,
                mongodb_waittime, mongodb_error, suspected_updates_or_blacklisted_records,
                difference_aero_mongo, status, log_path, minio_filepath, restart_point,
                load_type
            FROM {config_audit["schema"]}.{config_audit["audit_table"]}
            WHERE source_table = :src
            AND business_loaddt = TO_DATE(:aud_dt, :fmt)
            AND (
                (delta_column_value IS NULL AND :delta_val IS NULL)
                OR delta_column_value = :delta_val
            )
            AND NVL(load_type, 'delta') = :load_type
            ORDER BY updated_at_ts DESC NULLS LAST
            FETCH FIRST 1 ROW ONLY
        """
        params = {
            "src": audit_log["source_table"],
            "aud_dt": aud_dt,
            "fmt": ORACLE_DATE_FMT,
            "delta_val": delta_column_value,
            "load_type": audit_log.get("load_type", "delta")
        }
    
    with connect_to_oracle(config_audit["target"]) as conn:
        with conn.cursor() as cur:
            logger.debug("Fetching existing audit record for restart")
            cur.execute(query, params)
            row = cur.fetchone()
            if row:
                keys = [
                    "source_table", "task_startts", "task_endts", "task_exec_secs", "business_loaddt",
                    "delta_column_value", "total_records", "extraction_time", "total_apicalls",
                    "success_apicalls", "failed_apicalls", "api_failedpath", "apicall_time",
                    "cdp_db_count_validation", "aerospike_init_record_cnt", "aerospike_init_read_waittime",
                    "aerospike_record_cnt", "aerospike_waittime", "aerospike_error",
                    "mongodb_init_record_cnt", "mongodb_record_cnt", "mongodb_init_read_waittime",
                    "mongodb_waittime", "mongodb_error", "suspected_updates_or_blacklisted_records",
                    "difference_aero_mongo", "status", "log_path", "minio_filepath", "restart_point",
                    "load_type"
                ]
                
                existing_data = dict(zip(keys, row))
                audit_log.update(existing_data)
                
                # Handle CLOB fields with specific exception handling (MJ #10)
                for field in ["api_failedpath", "aerospike_error", "mongodb_error"]:
                    if isinstance(audit_log.get(field), LOB):
                        try:
                            audit_log[field] = audit_log[field].read()
                        except oracledb.Error as e:
                            logger.warning("Failed to read CLOB field %s: %s", field, e)
                            audit_log[field] = None
                
                logger.info("Existing audit record found - restart_point=%s status=%s delta_value=%s total_records=%s",
                           audit_log.get("restart_point"), audit_log.get("status"),
                           audit_log.get("delta_column_value"), audit_log.get("total_records"))
            else:
                logger.info("No existing audit record found - starting fresh")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def update_audit_record_strict(config_audit: Dict[str, Any], audit_data: Dict[str, Any]) -> None:
    """Strict audit update with proper composite key matching for historic loads."""
    audit_data = _ensure_time_fields(audit_data)
    
    if audit_data.get("load_type") == "historic":
        merge_sql = f"""
            MERGE INTO {config_audit['schema']}.{config_audit['audit_table']} target
            USING (
                SELECT
                    :source_table AS source_table,
                    :load_type AS load_type
                FROM dual
            ) src
            ON (
                target.source_table = src.source_table
                AND target.load_type = src.load_type
            )
            WHEN MATCHED THEN UPDATE SET
                delta_column_value = :delta_column_value,
                task_startts = :task_startts,
                task_endts = :task_endts,
                task_exec_secs = :task_exec_secs,
                total_records = :total_records,
                extraction_time = :extraction_time,
                total_apicalls = :total_apicalls,
                success_apicalls = :success_apicalls,
                failed_apicalls = :failed_apicalls,
                api_failedpath = :api_failedpath,
                apicall_time = :apicall_time,
                cdp_db_count_validation = :cdp_db_count_validation,
                aerospike_init_record_cnt = :aerospike_init_record_cnt,
                aerospike_init_read_waittime = :aerospike_init_read_waittime,
                aerospike_record_cnt = :aerospike_record_cnt,
                aerospike_waittime = :aerospike_waittime,
                aerospike_error = :aerospike_error,
                mongodb_init_record_cnt = :mongodb_init_record_cnt,
                mongodb_record_cnt = :mongodb_record_cnt,
                mongodb_init_read_waittime = :mongodb_init_read_waittime,
                mongodb_waittime = :mongodb_waittime,
                mongodb_error = :mongodb_error,
                suspected_updates_or_blacklisted_records = :suspected_updates_or_blacklisted_records,
                difference_aero_mongo = :difference_aero_mongo,
                status = :status,
                log_path = :log_path,
                minio_filepath = :minio_filepath,
                restart_point = :restart_point,
                updated_at_ts = :updated_at_ts,
                business_loaddt = :business_loaddt
            WHEN NOT MATCHED THEN INSERT (
                source_table, business_loaddt, delta_column_value, load_type, task_startts, task_endts,
                task_exec_secs, total_records, extraction_time, total_apicalls, success_apicalls,
                failed_apicalls, api_failedpath, apicall_time, cdp_db_count_validation,
                aerospike_init_record_cnt, aerospike_init_read_waittime, aerospike_record_cnt,
                aerospike_waittime, aerospike_error, mongodb_init_record_cnt, mongodb_record_cnt,
                mongodb_init_read_waittime, mongodb_waittime, mongodb_error,
                suspected_updates_or_blacklisted_records, difference_aero_mongo, status, log_path,
                minio_filepath, restart_point, created_at_ts, updated_at_ts
            ) VALUES (
                :source_table, :business_loaddt, :delta_column_value, :load_type, :task_startts, :task_endts,
                :task_exec_secs, :total_records, :extraction_time, :total_apicalls, :success_apicalls,
                :failed_apicalls, :api_failedpath, :apicall_time, :cdp_db_count_validation,
                :aerospike_init_record_cnt, :aerospike_init_read_waittime, :aerospike_record_cnt,
                :aerospike_waittime, :aerospike_error, :mongodb_init_record_cnt, :mongodb_record_cnt,
                :mongodb_init_read_waittime, :mongodb_waittime, :mongodb_error,
                :suspected_updates_or_blacklisted_records, :difference_aero_mongo, :status, :log_path,
                :minio_filepath, :restart_point, :created_at_ts, :updated_at_ts
            )
        """
    else:
        merge_sql = f"""
            MERGE INTO {config_audit['schema']}.{config_audit['audit_table']} target
            USING (
                SELECT
                    :source_table AS source_table,
                    :business_loaddt AS business_loaddt,
                    :delta_column_value AS delta_column_value,
                    :load_type AS load_type
                FROM dual
            ) src
            ON (
                target.source_table = src.source_table
                AND target.business_loaddt = src.business_loaddt
                AND (
                    (target.delta_column_value IS NULL AND src.delta_column_value IS NULL)
                    OR target.delta_column_value = src.delta_column_value
                )
                AND NVL(target.load_type, 'delta') = NVL(src.load_type, 'delta')
            )
            WHEN MATCHED THEN UPDATE SET
                task_startts = :task_startts,
                task_endts = :task_endts,
                task_exec_secs = :task_exec_secs,
                total_records = :total_records,
                extraction_time = :extraction_time,
                total_apicalls = :total_apicalls,
                success_apicalls = :success_apicalls,
                failed_apicalls = :failed_apicalls,
                api_failedpath = :api_failedpath,
                apicall_time = :apicall_time,
                cdp_db_count_validation = :cdp_db_count_validation,
                aerospike_init_record_cnt = :aerospike_init_record_cnt,
                aerospike_init_read_waittime = :aerospike_init_read_waittime,
                aerospike_record_cnt = :aerospike_record_cnt,
                aerospike_waittime = :aerospike_waittime,
                aerospike_error = :aerospike_error,
                mongodb_init_record_cnt = :mongodb_init_record_cnt,
                mongodb_record_cnt = :mongodb_record_cnt,
                mongodb_init_read_waittime = :mongodb_init_read_waittime,
                mongodb_waittime = :mongodb_waittime,
                mongodb_error = :mongodb_error,
                suspected_updates_or_blacklisted_records = :suspected_updates_or_blacklisted_records,
                difference_aero_mongo = :difference_aero_mongo,
                status = :status,
                log_path = :log_path,
                minio_filepath = :minio_filepath,
                restart_point = :restart_point,
                updated_at_ts = :updated_at_ts
            WHEN NOT MATCHED THEN INSERT (
                source_table, business_loaddt, delta_column_value, load_type, task_startts, task_endts,
                task_exec_secs, total_records, extraction_time, total_apicalls, success_apicalls,
                failed_apicalls, api_failedpath, apicall_time, cdp_db_count_validation,
                aerospike_init_record_cnt, aerospike_init_read_waittime, aerospike_record_cnt,
                aerospike_waittime, aerospike_error, mongodb_init_record_cnt, mongodb_record_cnt,
                mongodb_init_read_waittime, mongodb_waittime, mongodb_error,
                suspected_updates_or_blacklisted_records, difference_aero_mongo, status, log_path,
                minio_filepath, restart_point, created_at_ts, updated_at_ts
            ) VALUES (
                :source_table, :business_loaddt, :delta_column_value, :load_type, :task_startts, :task_endts,
                :task_exec_secs, :total_records, :extraction_time, :total_apicalls, :success_apicalls,
                :failed_apicalls, :api_failedpath, :apicall_time, :cdp_db_count_validation,
                :aerospike_init_record_cnt, :aerospike_init_read_waittime, :aerospike_record_cnt,
                :aerospike_waittime, :aerospike_error, :mongodb_init_record_cnt, :mongodb_record_cnt,
                :mongodb_init_read_waittime, :mongodb_waittime, :mongodb_error,
                :suspected_updates_or_blacklisted_records, :difference_aero_mongo, :status, :log_path,
                :minio_filepath, :restart_point, :created_at_ts, :updated_at_ts
            )
        """

    try:
        with connect_to_oracle(config_audit["target"]) as conn:
            conn.autocommit = False
            cur = conn.cursor()
            logger.debug("Updating audit for table=%s delta_value=%s status=%s",
                        audit_data.get("source_table"), audit_data.get("delta_column_value"), 
                        audit_data.get("status"))
            cur.execute(merge_sql, audit_data)
            conn.commit()
            logger.info("Audit update successful")
    except Exception as e:
        logger.error("CRITICAL: Audit update failed: %s", e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise RuntimeError(f"Audit update failed: {e}")  # Simplified error handling (MJ #16)
    finally:
        close_connection(cur, conn)

# Backward compatibility wrapper
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def update_audit_record(config_audit: Dict[str, Any], audit_data: Dict[str, Any]) -> None:
    """Wrapper for backward compatibility - uses strict blocking update."""
    update_audit_record_strict(config_audit, audit_data)

# Helper for audit record processing (MJ #20)
def _process_audit_rows(rows: List[Any], load_type: str, current_business_loaddt: str) -> List[Dict[str, Any]]:
    """Process audit table rows into job dictionaries."""
    processed: List[Dict[str, Any]] = []
    for row in rows:
        business_dt, status, restart_point, total_records, delta_val, load_type_row, task_exec_secs, extraction_time = row
        biz_str = business_dt.strftime("%Y-%m-%d") if hasattr(business_dt, "strftime") else str(business_dt)
        processed.append({
            "business_loaddt": biz_str,
            "status": status,
            "restart_point": int(restart_point or 0),
            "total_records": int(total_records or 0),
            "task_exec_secs": float(task_exec_secs or 0),
            "extraction_time": float(extraction_time or 0),
            "delta_column_value": delta_val,
            "load_type": load_type_row or load_type
        })
    return processed

# Refactored historic load status (MJ #17)
def _load_deltas_from_parquet(parquet_file: str, source_table: str, delta_column: str, 
                             oracle_config: Dict[str, Any]) -> pd.DataFrame:
    """Load or refresh distinct delta values from Parquet or source."""
    os.makedirs(os.path.dirname(parquet_file), exist_ok=True)
    if not os.path.exists(parquet_file) or (time.time() - os.path.getmtime(parquet_file)) > 86400:  # Reinstated 24-hour refresh (MJ #18)
        logger.info("Parquet file %s not found or outdated. Refreshing.", parquet_file)
        with connect_to_oracle(oracle_config) as source_conn:
            query = f"SELECT DISTINCT {delta_column} as delta_value FROM {source_table} WHERE {delta_column} IS NOT NULL ORDER BY {delta_column}"
            df = pd.read_sql(query, source_conn)
            df.to_parquet(parquet_file, index=False)
            logger.info("Saved refreshed delta values to %s.", parquet_file)
            return df
    logger.info("Using cached parquet file %s", parquet_file)
    return pd.read_parquet(parquet_file)

def _get_audit_record(config_audit: Dict[str, Any], source_table: str) -> Dict[str, Any]:
    """Get the latest audit record for historic loads."""
    query = f"""
        SELECT
            business_loaddt, NVL(status, 'NOT_STARTED') AS status,
            NVL(restart_point, 0) AS restart_point, NVL(total_records, 0) AS total_records,
            delta_column_value, NVL(load_type, 'historic') AS load_type,
            NVL(task_exec_secs, 0) AS task_exec_secs, NVL(extraction_time, 0) AS extraction_time,
            aerospike_error
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE source_table = :src
        AND load_type = 'historic'
        ORDER BY updated_at_ts DESC NULLS LAST
        FETCH FIRST 1 ROW ONLY
    """
    with connect_to_oracle(config_audit["target"]) as conn:
        cur = conn.cursor()
        cur.execute(query, {"src": source_table})
        row = cur.fetchone()
        if not row:
            return {
                "business_dt": None, "status": "NOT_STARTED", "restart_point": 0,
                "total_records": 0, "delta_val": None, "load_type": "historic",
                "task_exec_secs": 0, "extraction_time": 0, "aerospike_error": None
            }
        business_dt, status, restart_point, total_records, delta_val, load_type, task_exec_secs, extraction_time, aerospike_error = row
        if isinstance(aerospike_error, LOB):
            aerospike_error = aerospike_error.read()
        return {
            "business_dt": business_dt, "status": status, "restart_point": int(restart_point or 0),
            "total_records": int(total_records or 0), "delta_val": delta_val,
            "load_type": load_type, "task_exec_secs": float(task_exec_secs or 0),
            "extraction_time": float(extraction_time or 0), "aerospike_error": aerospike_error
        }

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def get_historic_load_status(config_audit: Dict[str, Any], source_table: str,
                           current_business_loaddt: str, delta_column: str,
                           oracle_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get historic load status, selecting unprocessed deltas from sorted Parquet file."""
    file_name = f'source_delta_{source_table.replace(".", "_")}.parquet'
    parquet_basepath = "/opt/airflow/etl_inward_files/CDP_minio/"
    parquet_file = os.path.join(parquet_basepath, file_name)
    
    try:
        # Load delta values
        source_deltas_df = _load_deltas_from_parquet(parquet_file, source_table, delta_column, oracle_config)
        all_deltas = source_deltas_df['delta_value'].astype(str).tolist()  # Fixed case (MJ #19)
        if not all_deltas:
            logger.info("No delta values found for %s in Parquet file.", source_table)
            return []
        
        # Get audit record
        audit_record = _get_audit_record(config_audit, source_table)
        base_status = audit_record["status"]
        base_restart_point = audit_record["restart_point"]
        base_total_records = audit_record["total_records"]
        base_exec_secs = audit_record["task_exec_secs"]
        base_extraction_time = audit_record["extraction_time"]
        base_delta_value = audit_record["delta_val"]
        completed_deltas = set(audit_record["aerospike_error"].split(",") if audit_record["aerospike_error"] else [])
        if base_status == "COMPLETED" and base_delta_value:
            completed_deltas.add(base_delta_value)
        
        # Compute unprocessed deltas
        try:
            processed_index = all_deltas.index(base_delta_value) if base_delta_value else -1
        except ValueError:
            processed_index = -1
        unprocessed_deltas = [d for d in all_deltas if d not in completed_deltas]
        
        # Refresh Parquet if no unprocessed deltas and last delta not completed
        if not unprocessed_deltas and all_deltas and all_deltas[-1] not in completed_deltas:
            logger.info("No unprocessed deltas, refreshing Parquet for backdated updates (MJ #18).")
            source_deltas_df = _load_deltas_from_parquet(parquet_file, source_table, delta_column, oracle_config)
            all_deltas = source_deltas_df['delta_value'].astype(str).tolist()
            unprocessed_deltas = [d for d in all_deltas if d not in completed_deltas]
        
        if not unprocessed_deltas:
            logger.info("All historic deltas completed for %s", source_table)
            return []
        
        logger.info("Found %s unprocessed deltas: %s", len(unprocessed_deltas), unprocessed_deltas)
        
        # Create job for each unprocessed delta
        processed_jobs: List[Dict[str, Any]] = []
        biz_str = audit_record["business_dt"].strftime("%Y-%m-%d") if audit_record["business_dt"] else current_business_loaddt
        
        for delta in unprocessed_deltas:
            if delta == base_delta_value and base_status != 'COMPLETED':
                processed_jobs.append({
                    "business_loaddt": biz_str,
                    "status": base_status,
                    "restart_point": base_restart_point,
                    "total_records": base_total_records,
                    "task_exec_secs": base_exec_secs,
                    "extraction_time": base_extraction_time,
                    "delta_column_value": delta,
                    "load_type": 'historic'
                })
            else:
                processed_jobs.append({
                    "business_loaddt": biz_str,
                    "status": "NOT_STARTED",
                    "restart_point": 0,
                    "total_records": base_total_records,
                    "task_exec_secs": base_exec_secs,
                    "extraction_time": base_extraction_time,
                    "delta_column_value": delta,
                    "load_type": 'historic'
                })
        
        logger.info("Next delta to process: %s (total_records=%d, status=%s)",
                   unprocessed_deltas[0], base_total_records, processed_jobs[0]["status"])
        return processed_jobs
                
    except Exception as e:
        logger.error("CRITICAL: Failed to get historic load status: %s", e, exc_info=True)
        raise RuntimeError(f"Historic load status query failed: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def get_delta_load_status(config_audit: Dict[str, Any], source_table: str,
                         current_business_loaddt: str) -> List[Dict[str, Any]]:
    """Get delta load status - only return incomplete jobs."""
    query = f"""
        SELECT
            business_loaddt, NVL(status, 'NOT_STARTED') AS status,
            NVL(restart_point, 0) AS restart_point, NVL(total_records, 0) AS total_records,
            delta_column_value, NVL(load_type, 'delta') AS load_type,
            NVL(task_exec_secs, 0) AS task_exec_secs, NVL(extraction_time, 0) AS extraction_time
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE business_loaddt <= TO_DATE(:current_dt, :fmt)
          AND source_table = :src
          AND NVL(load_type, 'delta') = 'delta'
          AND delta_column_value IS NULL
          AND NVL(status, 'NOT_STARTED') IN ('NOT_STARTED', 'FAILED', 'RUNNING')
        ORDER BY business_loaddt
    """

    with connect_to_oracle(config_audit["target"]) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(query, {"current_dt": current_business_loaddt, "fmt": ORACLE_DATE_FMT, "src": source_table})
                rows = cur.fetchall()
                processed = _process_audit_rows(rows, "delta", current_business_loaddt)
                
                if not processed:
                    completed_check_query = f"""
                        SELECT COUNT(*) FROM {config_audit["schema"]}.{config_audit["audit_table"]}
                        WHERE business_loaddt = TO_DATE(:current_dt, :fmt)
                          AND source_table = :src
                          AND NVL(load_type, 'delta') = 'delta'
                          AND delta_column_value IS NULL
                          AND status = 'COMPLETED'
                    """
                    cur.execute(completed_check_query, {"current_dt": current_business_loaddt, "fmt": ORACLE_DATE_FMT, "src": source_table})
                    completed_exists = cur.fetchone()[0] > 0
                    
                    if not completed_exists:
                        logger.info("No audit record found for %s on %s. Creating new entry.", source_table, current_business_loaddt)
                        processed.append({
                            "business_loaddt": current_business_loaddt,
                            "status": "NOT_STARTED",
                            "restart_point": 0,
                            "total_records": 0,
                            "task_exec_secs": 0,
                            "extraction_time": 0,
                            "delta_column_value": None,
                            "load_type": 'delta'
                        })
                    else:
                        logger.info("Job for %s on %s already COMPLETED. Skipping.", source_table, current_business_loaddt)
                
                return processed
            except Exception as e:
                logger.error("CRITICAL: Failed to get delta load status: %s", e, exc_info=True)
                raise RuntimeError(f"Delta load status query failed: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def get_load_status_and_dates(config_audit: Dict[str, Any], source_table: str,
                             current_business_loaddt: str, load_type: str = 'delta',
                             delta_column: Optional[str] = None,
                             oracle_config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Returns list of incomplete jobs only - skips COMPLETED jobs."""
    if load_type == 'historic' and delta_column and oracle_config:
        return get_historic_load_status(config_audit, source_table, current_business_loaddt, delta_column, oracle_config)
    return get_delta_load_status(config_audit, source_table, current_business_loaddt)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def create_audit_table_if_not_exists(config_audit: Dict[str, Any]) -> None:
    """Create audit table with proper constraints to prevent duplicates."""
    check_query = """
        SELECT COUNT(*)
        FROM all_tables
        WHERE table_name = UPPER(:tbl)
          AND owner = UPPER(:own)
    """

    with connect_to_oracle(config_audit["target"]) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(check_query, {"tbl": config_audit["audit_table"], "own": config_audit["schema"]})
                exists = cur.fetchone()[0]
                if exists == 0:
                    logger.warning("Audit table '%s' not found. Creating...", config_audit["audit_table"])
                    create_sql = f"""
                        CREATE TABLE {config_audit['schema']}.{config_audit['audit_table']} (
                            run_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            source_table VARCHAR2(100) NOT NULL,
                            task_startts TIMESTAMP,
                            task_endts TIMESTAMP,
                            task_exec_secs NUMBER,
                            business_loaddt DATE NOT NULL,
                            delta_column_value VARCHAR2(100),
                            total_records NUMBER DEFAULT 0,
                            extraction_time NUMBER,
                            total_apicalls NUMBER,
                            success_apicalls NUMBER,
                            failed_apicalls NUMBER,
                            api_failedpath CLOB,
                            apicall_time NUMBER,
                            cdp_db_count_validation VARCHAR2(10),
                            aerospike_init_record_cnt NUMBER,
                            aerospike_init_read_waittime NUMBER,
                            aerospike_record_cnt NUMBER,
                            aerospike_waittime NUMBER,
                            aerospike_error CLOB,
                            mongodb_init_record_cnt NUMBER,
                            mongodb_record_cnt NUMBER,
                            mongodb_init_read_waittime NUMBER,
                            mongodb_waittime NUMBER,
                            mongodb_error CLOB,
                            suspected_updates_or_blacklisted_records NUMBER,
                            difference_aero_mongo NUMBER,
                            status VARCHAR2(50),
                            log_path VARCHAR2(1000),
                            minio_filepath VARCHAR2(1000),
                            restart_point NUMBER DEFAULT 0,
                            load_type VARCHAR2(20) DEFAULT 'delta' NOT NULL,
                            created_at_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            CONSTRAINT uk_audit_composite UNIQUE (
                                source_table,
                                CASE WHEN load_type = 'delta' THEN business_loaddt ELSE NULL END,
                                CASE WHEN load_type = 'delta' THEN delta_column_value ELSE NULL END,
                                load_type
                            )
                        )
                    """
                    cur.execute(create_sql)
                    cur.execute(
                        f"CREATE INDEX idx_{config_audit['audit_table']}_status ON {config_audit['schema']}.{config_audit['audit_table']}(source_table, status, load_type)"
                    )
                    cur.execute(
                        f"CREATE INDEX idx_{config_audit['audit_table']}_loaddt ON {config_audit['schema']}.{config_audit['audit_table']}(business_loaddt, load_type)"
                    )
                    conn.commit()
                    logger.info("Audit table created with proper constraints.")
                else:
                    logger.info("Audit table exists. Checking for missing columns...")
                    required_columns = [
                        ("delta_column_value", "VARCHAR2(100)"),
                        ("load_type", "VARCHAR2(20) DEFAULT 'delta'")
                    ]
                    for col_name, col_type in required_columns:  # Simplified loop (MJ #22)
                        try:
                            cur.execute(f"SELECT {col_name} FROM {config_audit['schema']}.{config_audit['audit_table']} WHERE 1=0")
                        except oracledb.Error as e:
                            if "ORA-00904" in str(e):  # Column doesn't exist
                                logger.info("Adding missing column %s", col_name)
                                cur.execute(f"ALTER TABLE {config_audit['schema']}.{config_audit['audit_table']} ADD ({col_name} {col_type})")
                            else:
                                logger.warning("Could not verify column %s: %s", col_name, e)
                    conn.commit()
                    logger.info("Audit table updated successfully.")
            except Exception as e:
                logger.error("CRITICAL: Failed to create/update audit table: %s", e)
                raise RuntimeError(f"Audit table setup failed: {e}")

# MinIO upload helper
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def _upload_df_parquet(minio_client: MinioHandler, df: pd.DataFrame, object_path: str, 
                      compression: str = "snappy") -> None:
    """Upload DataFrame as parquet to MinIO."""
    try:
        return minio_client.upload_dataframe(df=df, object_path=object_path, 
                                           format="parquet", compression=compression)
    except AttributeError:
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=compression)
        data = buf.getvalue()
        if hasattr(minio_client, "put_object"):
            minio_client.put_object(object_path, data, len(data), content_type="application/octet-stream")
        else:
            raise RuntimeError("MinioHandler must provide upload_dataframe(...) or put_object(...)")

# Path Generation Helper
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

# Core Extraction Function
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
) -> None:
    """Extract data from Oracle to MinIO parquet with audit logging (MJ #24 simplified)."""
    if not table_name or not table_name.strip():
        raise ValueError("table_name is required and cannot be empty")
    if not base_object_path:
        raise ValueError("base_object_path is required")
    try:
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

    create_audit_table_if_not_exists(config_audit)
    audit_log = prepare_auditing(table_name.upper(), load_type, business_loaddt)
    audit_log.update({
        "task_startts": extraction_start.strftime(DATETIMEFORMAT),
        "status": "RUNNING",
        "minio_filepath": effective_object_path,
        "delta_column_value": delta_column_value,
    })

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
        update_audit_record_strict(config_audit, audit_log)
        start_chunk_index = 0
        total_records = previous_total
    else:
        start_chunk_index = max(int(restart_point or 0), int(audit_log.get("restart_point") or 0))
        total_records = int(audit_log.get("total_records") or 0)

    logger.info("Starting extraction for %s on %s (load_type=%s, delta_value=%s)",
               table_name, business_loaddt, load_type, delta_column_value)
    logger.info("Restart chunk index: %s | total_records so far: %s", start_chunk_index, total_records)

    where_part = f" WHERE {delta_column} = TO_DATE('{delta_column_value}', 'YYYY-MM-DD')" if load_type == 'historic' and delta_column and delta_column_value else ""
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT * FROM {table_name}{where_part}{order_clause}"

    conn = None
    cur = None
    try:
        conn = connect_to_oracle(oracle_config)
        mclient = MinioHandler(minio_config)
        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))
        logger.info("Executing SELECT for streaming: %s", select_sql)
        cur.execute(select_sql)

        for _ in range(start_chunk_index):
            skipped = cur.fetchmany(chunk_size)
            if not skipped:
                break

        chunk_index = start_chunk_index
        cols = [d[0] for d in cur.description]

        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                logger.info("No more data to process for %s", table_name)
                break

            df_chunk = pd.DataFrame(rows, columns=cols)
            clean_delta = str(delta_column_value).replace("-", "") if delta_column_value else ""
            object_name = (
                f"{effective_object_path}/{table_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
                if load_type == 'historic'
                else f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
            ).replace("//", "/")

            try:
                _upload_df_parquet(mclient, df_chunk, object_name, compression=compression)
                recs = len(df_chunk)
                if chunk_index % 10 == 0:
                    logger.info("Successfully uploaded chunk %s (%s rows)", chunk_index, recs)
            except Exception as e:
                logger.error("Failed to upload chunk %s to MinIO: %s", chunk_index, e)
                raise RuntimeError(f"MinIO upload failed for chunk {chunk_index}: {e}")

            now_ist = datetime.now(IST)
            current_run_recs = len(df_chunk)
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
                logger.error("Audit update failed for chunk %s; deleting uploaded object: %s/%s", 
                           chunk_index, bucket, object_name)
                try:
                    if hasattr(mclient, "delete_file") and bucket:
                        mclient.delete_file(object_name, bucket)
                    elif hasattr(mclient, "remove_object") and bucket:
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
            logger.info("Completed Oracle -> MinIO parquet for %s (load_type=%s)", table_name, load_type)
        except Exception as e:
            logger.error("Final audit update failed: %s", e)
            raise RuntimeError(f"Final audit update failed: {e}")

        mclient.close()

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

# Sequential Processing
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
) -> None:
    """Sequential processing - processes ALL incomplete jobs in order, stopping if any fails."""
    table_name_uc = table_name.upper()
    logger.info("Begin processing Oracle->MinIO for %s up to %s (load_type=%s)",
               table_name_uc, current_business_loaddt, load_type)

    dates = get_load_status_and_dates(
        config_audit, table_name_uc, current_business_loaddt, 
        load_type, delta_column, oracle_config if load_type == 'historic' else None
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
            )
            logger.info("Successfully completed processing for job %s delta_value=%s", biz_dt, delta_value)

    logger.info("All required processing completed for %s", table_name_uc)

# Configuration-based processor
def process_from_config(config: Dict[str, Any], conn_config: Dict[str, Any],
                       current_business_loaddt: str) -> None:
    """Process all objects defined in configuration."""
    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})
    logger.info("MinIO config loaded: %s", {k: v for k, v in minio_config.items() if k != 'secret_key'})
    
    audit_config = {
        "target": oracle_config,
        "schema": config.get("schema", "uds"),
        "audit_table": config.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }
    
    for obj_config in config.get("objects", []):
        if not obj_config.get("isactive", True):
            logger.info("Skipping inactive object: %s", obj_config.get("db_table"))
            continue
            
        table_name = f"{obj_config.get('schema', config.get('schema'))}.{obj_config['db_table']}"
        output_path = obj_config.get("output_path", config.get("output_path"))
        load_type = obj_config.get("load_type", "delta")  ## MJ - TODO: Clarify how to determine load_type dynamically (e.g., based on table metadata) (MJ #25)
        chunk_size = obj_config.get("chunksize", config.get("chunksize", 100_000))
        order_by = obj_config.get("ORDER_BY")
        delta_column = obj_config.get("delta_column") if load_type == 'historic' else None
        sub_folder = obj_config.get("sub_folder") if load_type == 'historic' else None  ## MJ - TODO: Clarify if dynamic sub_folder creation is needed and how (MJ #26)
        
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
            )
        except Exception as e:
            logger.error("Failed to process %s: %s", table_name, e)
            raise RuntimeError(f"Object processing failed for {table_name}: {e}")

# Main execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    
    try:
        conn_config = get_system_config()
        config_yaml = "etl_configs/db2_to_uds_config.yml"
        config_data = load_config_from_yaml(config_yaml)["uds_to_minio"]  # Used load_config_from_yaml (MJ #3, #27)
        current_date = datetime.now(IST).strftime("%Y-%m-%d")
        
        logger.info("Starting configuration-based processing")
        process_from_config(config_data, conn_config, current_date)
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error("CRITICAL: Main execution failed: %s", e)
        raise RuntimeError(f"Application failed: {e}")
