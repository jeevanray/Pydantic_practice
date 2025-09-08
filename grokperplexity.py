import os
import io
import logging
import time
from typing import Dict, Any, List, Optional, Union, Sequence

import pandas as pd
import oracledb
import pytz
from datetime import datetime
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# External dependency expected by the caller's environment
from minio_handler import MinioHandler
from cdp_diapi_adapter import get_system_config

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logger = logging.getLogger("Minio_framework")

# -----------------------------------------------------------------------------
# Constants & TZ
# -----------------------------------------------------------------------------
DATETIMEFORMAT = "%Y-%m-%d %H:%M:%S"
ORACLE_DATE_FMT = "YYYY-MM-DD"
IST = pytz.timezone("Asia/Kolkata")

# -----------------------------------------------------------------------------
# Configuration Helper
# -----------------------------------------------------------------------------
def load_config_from_yaml(yaml_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# -----------------------------------------------------------------------------
# DB Helpers with mandatory error raising
# -----------------------------------------------------------------------------
def connect_to_oracle(oracle_conf: Dict[str, Any]) -> oracledb.Connection:
    """Connect to Oracle database with retry logic and MANDATORY error raising."""
    logger.debug("Connecting to Oracle DB")
    attempt = 0
    last_err: Optional[Exception] = None
    
    while attempt < 3:
        try:
            conn = oracledb.connect(
                user=oracle_conf.get("username", oracle_conf.get("user", "")),
                password=oracle_conf["password"],
                dsn=oracle_conf["dsn"],
            )
            logger.info(f"Oracle connection successful on attempt {attempt + 1}")
            return conn
        except Exception as e:
            attempt += 1
            last_err = e
            logger.error("[Connection Attempt %s] Oracle connect failed: %s", attempt, str(e))
            if attempt < 3:
                time.sleep(5)
    
    # MANDATORY: Always raise exception after all retries fail
    error_msg = f"CRITICAL: Failed to connect to Oracle database after {attempt} attempts"
    logger.error(error_msg)
    if last_err:
        raise last_err
    else:
        raise RuntimeError(error_msg)

def close_connection(cursor, connection):
    """Close database cursor/connection safely."""
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logger.debug("Closed Oracle connection")
    except Exception as e:
        logger.error("Error closing Oracle connection: %s", e)

# -----------------------------------------------------------------------------
# FIXED: Audit Helpers with Proper Delta Filtering
# -----------------------------------------------------------------------------
def prepare_auditing() -> Dict[str, Any]:
    """Base audit log dictionary with all expected keys present."""
    return {
        "source_table": "",
        "task_startts": "",
        "task_endts": "",
        "task_exec_secs": 0,
        "business_loaddt": "",
        "delta_column_value": None,
        "total_records": 0,
        "extraction_time": 0,
        "total_apicalls": 0,
        "success_apicalls": 0,
        "failed_apicalls": 0,
        "api_failedpath": None,
        "apicall_time": 0,
        "cdp_db_count_validation": False,
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
        "load_type": "delta",
        "created_at_ts": None,
        "updated_at_ts": None,
    }

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def initialize_restart_audit_log(config_audit: Dict[str, Any], audit_log: Dict[str, Any], aud_dt: str,
                                delta_column_value: Optional[str] = None) -> None:
    """
    FIXED: Load existing audit record for restart - properly filters by delta_column_value for historic loads.
    """
    
    # CRITICAL FIX: For historic loads, filter by BOTH source_table AND delta_column_value
    if audit_log.get("load_type") == "historic" and delta_column_value:
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
              AND delta_column_value = :delta_val
              AND load_type = 'historic'
            ORDER BY updated_at_ts DESC NULLS LAST
            FETCH FIRST 1 ROW ONLY
        """
        params = {
            "src": audit_log["source_table"],
            "delta_val": delta_column_value
        }
        logger.debug("FIXED: Querying audit for historic load with source_table=%s and delta_value=%s", 
                    audit_log["source_table"], delta_column_value)
    else:
        # Original logic for delta loads
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
                  (delta_column_value IS NULL AND :delta_val IS NULL) OR
                  (delta_column_value = :delta_val)
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
                
                # Handle CLOB fields
                try:
                    from oracledb import LOB
                    for field in ["api_failedpath", "aerospike_error", "mongodb_error"]:
                        if isinstance(audit_log.get(field), LOB):
                            audit_log[field] = audit_log[field].read()
                except Exception:
                    pass
                
                logger.info("FIXED: Found audit record for delta_value=%s - restart_point=%s status=%s",
                           audit_log.get("delta_column_value"), audit_log.get("restart_point"), 
                           audit_log.get("status"))
            else:
                logger.info("FIXED: No audit record found for delta_value=%s - starting fresh", delta_column_value)

def _ensure_time_fields(audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize types for time fields and booleans prior to MERGE."""
    audit_data["cdp_db_count_validation"] = 'Y' if audit_data.get("cdp_db_count_validation") else 'N'

    for k in ("task_startts", "task_endts"):
        v = audit_data.get(k)
        if isinstance(v, str) and v:
            audit_data[k] = IST.localize(datetime.strptime(v, DATETIMEFORMAT))
    
    v = audit_data.get("business_loaddt")
    if isinstance(v, str) and v:
        audit_data["business_loaddt"] = datetime.strptime(v, "%Y-%m-%d").date()

    now_ist = datetime.now(IST)
    audit_data["updated_at_ts"] = now_ist
    if not audit_data.get("created_at_ts"):
        audit_data["created_at_ts"] = now_ist
    return audit_data

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))  
def update_audit_record_strict(config_audit: Dict[str, Any], audit_data: Dict[str, Any], 
                              max_attempts: int = 5, wait_seconds: int = 3) -> None:
    """
    FIXED: Strict audit update with proper composite key matching for historic loads.
    """
    audit_data = _ensure_time_fields(audit_data)
    
    # FIXED: For historic loads, use delta_column_value as primary key component
    if audit_data.get("load_type") == "historic":
        merge_sql = f"""
            MERGE INTO {config_audit['schema']}.{config_audit['audit_table']} target
            USING (
                SELECT
                    :source_table AS source_table,
                    :delta_column_value AS delta_column_value,
                    :load_type AS load_type
                FROM dual
            ) src
            ON (
                target.source_table = src.source_table
                AND target.delta_column_value = src.delta_column_value
                AND target.load_type = src.load_type
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
        # Original delta load merge logic
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

    last_error = None
    for attempt in range(max_attempts):
        conn = None
        cur = None
        try:
            conn = connect_to_oracle(config_audit["target"])
            conn.autocommit = False
            cur = conn.cursor()
            
            logger.debug(
                "FIXED audit update (attempt %s/%s) for table=%s delta_value=%s status=%s",
                attempt + 1, max_attempts, audit_data.get("source_table"), 
                audit_data.get("delta_column_value"), audit_data.get("status")
            )
            
            cur.execute(merge_sql, audit_data)
            conn.commit()
            
            logger.info("FIXED audit update successful on attempt %s", attempt + 1)
            return  # Success - exit function
            
        except Exception as e:
            last_error = e
            logger.error("FIXED audit update failed on attempt %s/%s: %s", attempt + 1, max_attempts, e)
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if attempt < max_attempts - 1:
                logger.warning("Waiting %s seconds before retry...", wait_seconds)
                time.sleep(wait_seconds)
        finally:
            close_connection(cur, conn)
    
    # All attempts failed - raise error
    error_msg = f"CRITICAL: Audit update failed after {max_attempts} attempts - JOB MUST FAIL"
    logger.error(error_msg)
    if last_error:
        raise RuntimeError(f"{error_msg}: {last_error}")
    else:
        raise RuntimeError(error_msg)

# Backward compatibility wrapper
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def update_audit_record(config_audit: Dict[str, Any], audit_data: Dict[str, Any]) -> None:
    """Wrapper for backward compatibility - uses strict blocking update."""
    update_audit_record_strict(config_audit, audit_data)

# Keep all the other functions from your original code unchanged:
# - get_historic_load_status
# - get_load_status_and_dates  
# - get_delta_load_status
# - create_audit_table_if_not_exists
# - _upload_df_parquet
# - generate_object_path
# - oracle_to_minio_parquet
# - process_oracle_to_minio_with_dependencies
# - process_from_config
# - main execution

# ... [REST OF YOUR CODE REMAINS THE SAME] ...
