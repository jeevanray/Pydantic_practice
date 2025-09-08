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
# FIXED: Audit Helpers - Simplified with Proper Accumulation
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
        "incremental_records": 0,  # New: for current processing session
        "extraction_time": 0,
        "incremental_extraction_time": 0,  # New: for current processing session
        "total_apicalls": 0,
        "incremental_apicalls": 0,  # New: for current processing session
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
    """FIXED: Load existing audit record for restart - gets cumulative record for historic loads."""
    
    if audit_log.get("load_type") == "historic":
        # FIXED: For historic loads, get the cumulative audit record by source_table + load_type only
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
        logger.debug("FIXED: Getting cumulative audit for historic: source_table=%s", audit_log["source_table"])
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
                
                # FIXED: Store existing cumulative totals separately
                existing_total_records = existing_data.get("total_records", 0)
                existing_extraction_time = existing_data.get("extraction_time", 0)
                
                audit_log.update(existing_data)
                
                # Handle CLOB fields
                try:
                    from oracledb import LOB
                    for field in ["api_failedpath", "aerospike_error", "mongodb_error"]:
                        if isinstance(audit_log.get(field), LOB):
                            audit_log[field] = audit_log[field].read()
                except Exception:
                    pass
                
                logger.info("FIXED: Found cumulative audit - total_records=%s extraction_time=%.2f last_delta=%s status=%s",
                           existing_total_records, existing_extraction_time or 0, 
                           audit_log.get("delta_column_value"), audit_log.get("status"))
            else:
                logger.info("FIXED: No cumulative audit record found - starting fresh")

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
    FIXED: Proper cumulative audit update - accumulates metrics for historic loads.
    """
    audit_data = _ensure_time_fields(audit_data)
    
    if audit_data.get("load_type") == "historic":
        # FIXED: Proper accumulative MERGE for historic loads
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
                task_startts = CASE WHEN target.task_startts IS NULL THEN :task_startts ELSE target.task_startts END,
                task_endts = :task_endts,
                task_exec_secs = :task_exec_secs,
                business_loaddt = :business_loaddt,
                total_records = NVL(target.total_records, 0) + :incremental_records,
                extraction_time = NVL(target.extraction_time, 0) + :incremental_extraction_time,
                total_apicalls = NVL(target.total_apicalls, 0) + :incremental_apicalls,
                success_apicalls = NVL(target.success_apicalls, 0) + :success_apicalls,
                failed_apicalls = NVL(target.failed_apicalls, 0) + :failed_apicalls,
                api_failedpath = :api_failedpath,
                apicall_time = NVL(target.apicall_time, 0) + :apicall_time,
                cdp_db_count_validation = :cdp_db_count_validation,
                aerospike_init_record_cnt = NVL(target.aerospike_init_record_cnt, 0) + :aerospike_init_record_cnt,
                aerospike_init_read_waittime = NVL(target.aerospike_init_read_waittime, 0) + :aerospike_init_read_waittime,
                aerospike_record_cnt = NVL(target.aerospike_record_cnt, 0) + :aerospike_record_cnt,
                aerospike_waittime = NVL(target.aerospike_waittime, 0) + :aerospike_waittime,
                aerospike_error = :aerospike_error,
                mongodb_init_record_cnt = NVL(target.mongodb_init_record_cnt, 0) + :mongodb_init_record_cnt,
                mongodb_record_cnt = NVL(target.mongodb_record_cnt, 0) + :mongodb_record_cnt,
                mongodb_init_read_waittime = NVL(target.mongodb_init_read_waittime, 0) + :mongodb_init_read_waittime,
                mongodb_waittime = NVL(target.mongodb_waittime, 0) + :mongodb_waittime,
                mongodb_error = :mongodb_error,
                suspected_updates_or_blacklisted_records = NVL(target.suspected_updates_or_blacklisted_records, 0) + :suspected_updates_or_blacklisted_records,
                difference_aero_mongo = NVL(target.difference_aero_mongo, 0) + :difference_aero_mongo,
                status = :status,
                log_path = :log_path,
                minio_filepath = :minio_filepath,
                restart_point = :restart_point,
                delta_column_value = CASE WHEN :status = 'COMPLETED' THEN :delta_column_value ELSE target.delta_column_value END,
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
                :task_exec_secs, :incremental_records, :incremental_extraction_time, :incremental_apicalls, :success_apicalls,
                :failed_apicalls, :api_failedpath, :apicall_time, :cdp_db_count_validation,
                :aerospike_init_record_cnt, :aerospike_init_read_waittime, :aerospike_record_cnt,
                :aerospike_waittime, :aerospike_error, :mongodb_init_record_cnt, :mongodb_record_cnt,
                :mongodb_init_read_waittime, :mongodb_waittime, :mongodb_error,
                :suspected_updates_or_blacklisted_records, :difference_aero_mongo, :status, :log_path,
                :minio_filepath, :restart_point, :created_at_ts, :updated_at_ts
            )
        """
        
        # FIXED: Set incremental values correctly
        incremental_records = audit_data.get("incremental_records", 0)
        incremental_time = audit_data.get("incremental_extraction_time", 0)
        incremental_calls = audit_data.get("incremental_apicalls", 0)
        
        # Store incremental values in audit_data for the merge
        audit_data["incremental_records"] = incremental_records
        audit_data["incremental_extraction_time"] = incremental_time
        audit_data["incremental_apicalls"] = incremental_calls
        
    else:
        # Original delta load merge logic (unchanged)
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
            
            if audit_data.get("load_type") == "historic":
                incremental = audit_data.get("incremental_records", 0)
                delta_val = audit_data.get("delta_column_value")
                logger.debug(
                    "FIXED cumulative audit update (attempt %s/%s) for delta=%s incremental_records=%s status=%s",
                    attempt + 1, max_attempts, delta_val, incremental, audit_data.get("status")
                )
            else:
                logger.debug(
                    "FIXED audit update (attempt %s/%s) for table=%s loaddt=%s status=%s",
                    attempt + 1, max_attempts, audit_data.get("source_table"), 
                    audit_data.get("business_loaddt"), audit_data.get("status")
                )
            
            # Use SERIALIZABLE isolation to prevent race conditions
            cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            lock_query = f"""
                SELECT 1 FROM {config_audit['schema']}.{config_audit['audit_table']}
                WHERE source_table = :source_table
                  AND load_type = :load_type
                FOR UPDATE
            """ if audit_data.get("load_type") == "historic" else f"""
                SELECT 1 FROM {config_audit['schema']}.{config_audit['audit_table']}
                WHERE source_table = :source_table
                  AND business_loaddt = :business_loaddt
                  AND NVL(delta_column_value, 'NULL') = NVL(:delta_column_value, 'NULL')
                  AND NVL(load_type, 'delta') = :load_type
                FOR UPDATE
            """
            cur.execute(lock_query, {
                "source_table": audit_data["source_table"],
                "business_loaddt": audit_data["business_loaddt"],
                "delta_column_value": audit_data["delta_column_value"],
                "load_type": audit_data.get("load_type", "delta")
            })
            
            cur.execute(merge_sql, audit_data)
            conn.commit()
            
            # FIXED: Enhanced logging for cumulative updates
            if audit_data.get("load_type") == "historic":
                incremental = audit_data.get("incremental_records", 0)
                delta_val = audit_data.get("delta_column_value")
                status = audit_data.get("status")
                logger.info("FIXED cumulative audit update successful - delta=%s added_records=%s status=%s (attempt %s)", 
                           delta_val, incremental, status, attempt + 1)
            else:
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

# FIXED: Simplified historic load status - no complex logic
def get_historic_load_status(config_audit: Dict[str, Any], source_table: str,
                           current_business_loaddt: str, delta_column: str,
                           oracle_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    FIXED: Simplified historic load status - returns ONLY unprocessed deltas.
    """
    file_name = f'source_delta_{source_table.replace(".", "_")}.parquet'
    parquet_basepath = "/opt/airflow/etl_inward_files/CDP_minio/"
    parquet_file = os.path.join(parquet_basepath, file_name)
    os.makedirs(parquet_basepath, exist_ok=True)
    
    try:
        # Get all delta values from source table
        with connect_to_oracle(oracle_config) as source_conn:
            if not os.path.exists(parquet_file) or (time.time() - os.path.getmtime(parquet_file)) > 86400:
                logger.info("Refreshing delta values from source table %s", source_table)
                query = f"SELECT DISTINCT {delta_column} as delta_value FROM {source_table} WHERE {delta_column} IS NOT NULL ORDER BY {delta_column}"
                source_deltas_df = pd.read_sql(query, source_conn)
                source_deltas_df.to_parquet(parquet_file, index=False)
                logger.info("Cached %s delta values to %s", len(source_deltas_df), parquet_file)
            else:
                source_deltas_df = pd.read_parquet(parquet_file)
        
        all_deltas = source_deltas_df['DELTA_VALUE'].astype(str).tolist()
        
        # Get COMPLETED deltas from audit table
        with connect_to_oracle(config_audit["target"]) as audit_conn:
            completed_deltas_query = f"""
                SELECT DISTINCT delta_column_value
                FROM {config_audit["schema"]}.{config_audit["audit_table"]}
                WHERE source_table = :src
                AND load_type = 'historic'
                AND status = 'COMPLETED'
                AND delta_column_value IS NOT NULL
            """
            completed_deltas_df = pd.read_sql(completed_deltas_query, audit_conn, params={"src": source_table})
            completed_deltas = set(completed_deltas_df['DELTA_COLUMN_VALUE'].astype(str).tolist()) if not completed_deltas_df.empty else set()
        
        # FIXED: Simple logic - find unprocessed deltas
        unprocessed_deltas = [d for d in all_deltas if d not in completed_deltas]
        
        if not unprocessed_deltas:
            logger.info("FIXED: All historic deltas completed for %s", source_table)
            return []
        
        logger.info("FIXED: Found %s unprocessed deltas: %s", len(unprocessed_deltas), unprocessed_deltas[:5])
        
        # Return simple list of unprocessed deltas
        jobs = []
        for delta_val in unprocessed_deltas:
            jobs.append({
                "business_loaddt": current_business_loaddt,
                "status": "NOT_STARTED",
                "restart_point": 0,
                "total_records": 0,
                "delta_column_value": delta_val,
                "load_type": 'historic'
            })
                
        logger.info("FIXED: Returning %s historic jobs to process", len(jobs))
        return jobs
                
    except Exception as e:
        logger.error("CRITICAL: Failed to get historic load status: %s", e, exc_info=True)
        raise RuntimeError(f"Historic load status query failed: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def get_load_status_and_dates(config_audit: Dict[str, Any], source_table: str,
                             current_business_loaddt: str, load_type: str = 'delta',
                             delta_column: Optional[str] = None,
                             oracle_config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Returns list of incomplete jobs only - skips COMPLETED jobs."""
    if load_type == 'historic' and delta_column and oracle_config:
        return get_historic_load_status(config_audit, source_table, current_business_loaddt, delta_column, oracle_config)
    else:
        return get_delta_load_status(config_audit, source_table, current_business_loaddt)

def get_delta_load_status(config_audit: Dict[str, Any], source_table: str,
                         current_business_loaddt: str) -> List[Dict[str, Any]]:
    """Get delta load status - only return incomplete jobs."""
    query = f"""
        SELECT
            business_loaddt,
            NVL(status, 'NOT_STARTED') AS status,
            NVL(restart_point, 0) AS restart_point,
            NVL(total_records, 0) AS total_records,
            delta_column_value,
            NVL(load_type, 'delta') AS load_type
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
                processed: List[Dict[str, Any]] = []
                
                for business_dt, status, restart_point, total_records, delta_val, load_type in rows:
                    biz_str = business_dt.strftime("%Y-%m-%d") if hasattr(business_dt, "strftime") else str(business_dt)
                    processed.append({
                        "business_loaddt": biz_str,
                        "status": status,
                        "restart_point": int(restart_point or 0),
                        "total_records": int(total_records or 0),
                        "delta_column_value": delta_val,
                        "load_type": load_type
                    })
                
                if not processed:
                    # Check if COMPLETED record exists
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
                    
                    # Create performance indexes
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
                    try:
                        cur.execute(f"SELECT delta_column_value, load_type FROM {config_audit['schema']}.{config_audit['audit_table']} WHERE 1=0")
                        logger.info("All required columns exist in audit table.")
                    except Exception:
                        logger.info("Adding missing columns to existing audit table...")
                        try:
                            cur.execute(f"ALTER TABLE {config_audit['schema']}.{config_audit['audit_table']} ADD (delta_column_value VARCHAR2(100))")
                            logger.info("Added DELTA_COLUMN_VALUE column")
                        except Exception as e:
                            if "ORA-01430" not in str(e):
                                logger.warning("Could not add DELTA_COLUMN_VALUE: %s", e)
                        
                        try:
                            cur.execute(f"ALTER TABLE {config_audit['schema']}.{config_audit['audit_table']} ADD (load_type VARCHAR2(20) DEFAULT 'delta')")
                            logger.info("Added LOAD_TYPE column")
                        except Exception as e:
                            if "ORA-01430" not in str(e):
                                logger.warning("Could not add LOAD_TYPE: %s", e)
                        
                        conn.commit()
                        logger.info("Audit table updated successfully.")
                
            except Exception as e:
                logger.error("CRITICAL: Failed to create/update audit table: %s", e)
                raise RuntimeError(f"Audit table setup failed: {e}")

# -----------------------------------------------------------------------------
# MinIO upload helper (unchanged)
# -----------------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def _upload_df_parquet(minio_client: MinioHandler, df: pd.DataFrame, object_path: str, 
                      compression: str = "snappy") -> None:
    """Upload DataFrame as parquet to MinIO."""
    try:
        return minio_client.upload_dataframe(df=df, object_path=object_path, 
                                           format="parquet", compression=compression)
    except AttributeError:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:
            raise RuntimeError("pyarrow is required for parquet upload fallback") from e
        
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=compression)
        data = buf.getvalue()
        
        if hasattr(minio_client, "put_object"):
            minio_client.put_object(object_path, data, len(data), content_type="application/octet-stream")
        else:
            raise RuntimeError("MinioHandler must provide upload_dataframe(...) or put_object(...)")

# -----------------------------------------------------------------------------
# Path Generation Helper (unchanged)
# -----------------------------------------------------------------------------
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
            # Clean delta value for path usage
            clean_delta = str(delta_column_value).replace("-", "").replace(":", "").replace(" ", "")
            return f"{base_path}/history/{clean_delta}"
        else:
            raise ValueError("delta_column_value is required for historic load type")
    else:
        return f"{base_path}/delta/{date_folder}"

# -----------------------------------------------------------------------------
# FIXED: Core Extraction Function with Proper Incremental Tracking
# -----------------------------------------------------------------------------
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
    select_columns: Optional[Sequence[str]] = None,
    mapping_column: Optional[str] = None,
    delta_columns: Optional[Sequence[str]] = None,
    load_type: str = 'delta',
    delta_column: Optional[str] = None,
    delta_column_value: Optional[str] = None,
    sub_folder: Optional[str] = None,
    where_clause: Optional[str] = None,
) -> None:
    """
    FIXED: Enhanced extraction with proper cumulative tracking for historic loads.
    """
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

    # Generate effective object path based on load type
    effective_object_path = generate_object_path(
        base_object_path, business_loaddt, load_type, delta_column_value, sub_folder
    )
    effective_object_path = effective_object_path.replace("//", "/")

    # Ensure audit table exists
    create_audit_table_if_not_exists(config_audit)

    # Initialize audit log
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

    # Check for existing record and restart from there
    initialize_restart_audit_log(config_audit, audit_log, business_loaddt, delta_column_value)

    # Use higher of provided restart_point vs audit restart_point
    start_chunk_index = max(int(restart_point or 0), int(audit_log.get("restart_point") or 0))
    
    # FIXED: For historic loads, track existing totals and current delta processing separately
    existing_total_records = int(audit_log.get("total_records") or 0)
    existing_extraction_time = float(audit_log.get("extraction_time") or 0)
    current_delta_records = 0  # Records processed for current delta only

    # Skip if already completed
    if audit_log.get("status") == "COMPLETED":
        logger.info("Job already COMPLETED for %s load_type=%s delta_value=%s. Skipping.",
                   table_name, load_type, delta_column_value)
        return

    logger.info("FIXED: Starting extraction for %s on %s (load_type=%s, delta_value=%s)",
               table_name, business_loaddt, load_type, delta_column_value)
    
    if load_type == 'historic':
        logger.info("FIXED: Existing cumulative totals - records=%s extraction_time=%.2f", 
                   existing_total_records, existing_extraction_time)
        logger.info("FIXED: Restart chunk index: %s for delta %s", start_chunk_index, delta_column_value)
    else:
        logger.info("FIXED: Restart chunk index: %s | total_records so far: %s", start_chunk_index, existing_total_records)

    # Build SELECT with deterministic ordering and optional WHERE clause
    where_part = ""
    if load_type == 'historic' and delta_column and delta_column_value:
        where_part = f" WHERE {delta_column} = TO_DATE('{delta_column_value}', 'YYYY-MM-DD')"
    elif where_clause:
        where_part = f" WHERE {where_clause}"
    
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT * FROM {table_name}{where_part}{order_clause}"

    conn = None
    cur = None

    try:
        # Connection + MinIO client
        conn = connect_to_oracle(oracle_config)
        mclient = MinioHandler(minio_config)
        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))
        logger.info("FIXED: Executing SELECT for streaming: %s", select_sql)
        cur.execute(select_sql)

        # Fast-forward by discarding already-processed chunks
        for _ in range(start_chunk_index):
            skipped = cur.fetchmany(chunk_size)
            if not skipped:
                break

        chunk_index = start_chunk_index
        cols = [d[0] for d in cur.description]

        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                logger.info("FIXED: No more data to process for %s delta %s", table_name, delta_column_value or "N/A")
                break

            df_chunk = pd.DataFrame(rows, columns=cols)
            
            # Generate object name based on load type
            if load_type == 'historic' and delta_column_value:
                clean_delta = str(delta_column_value).replace("-", "")
                object_name = f"{effective_object_path}/{table_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
            else:
                object_name = f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
            
            object_name = object_name.replace("//", "/")

            # Upload parquet chunk to MinIO
            try:
                _upload_df_parquet(mclient, df_chunk, object_name, compression=compression)
                recs = len(df_chunk)
                current_delta_records += recs  # Track for current delta
                
                if chunk_index % 10 == 0:  # Log every 10th chunk to reduce verbosity
                    if load_type == 'historic':
                        logger.info("FIXED: Successfully uploaded chunk %s (%s rows) for delta %s", 
                                   chunk_index, recs, delta_column_value)
                    else:
                        logger.info("FIXED: Successfully uploaded chunk %s (%s rows)", chunk_index, recs)
            except Exception as e:
                logger.error("CRITICAL: Failed to upload chunk %s to MinIO: %s", chunk_index, e)
                raise RuntimeError(f"MinIO upload failed for chunk {chunk_index}: {e}")

            # Prepare audit update with incremental values
            now_ist = datetime.now(IST)
            current_extraction_time = (now_ist - extraction_start).total_seconds()
            
            # FIXED: For historic loads, set incremental values for cumulative tracking
            if load_type == 'historic':
                audit_log.update({
                    "incremental_records": recs,  # Records in this chunk only
                    "incremental_extraction_time": 0,  # We'll set this at the end
                    "incremental_apicalls": 0,  # Set based on actual API calls if any
                    "status": "RUNNING",
                    "task_endts": now_ist.strftime(DATETIMEFORMAT),
                    "task_exec_secs": current_extraction_time,
                    "extraction_time": current_extraction_time,  # Current session time
                    "restart_point": chunk_index + 1,
                    "minio_filepath": effective_object_path,
                })
            else:
                # Original logic for delta loads
                audit_log.update({
                    "total_records": current_delta_records,
                    "status": "RUNNING",
                    "task_endts": now_ist.strftime(DATETIMEFORMAT),
                    "task_exec_secs": current_extraction_time,
                    "extraction_time": current_extraction_time,
                    "restart_point": chunk_index + 1,
                    "minio_filepath": effective_object_path,
                })

            # FIXED: Mandatory blocking audit update
            try:
                update_audit_record_strict(config_audit, audit_log, max_attempts=5, wait_seconds=3)
                logger.debug("FIXED: Chunk %s audit update completed successfully", chunk_index)
            except Exception as e:
                logger.error("CRITICAL: Audit update failed for chunk %s; deleting uploaded object: %s/%s", 
                           chunk_index, bucket, object_name)
                # Clean up uploaded object
                try:
                    if hasattr(mclient, "delete_file") and bucket:
                        mclient.delete_file(object_name, bucket)
                    elif hasattr(mclient, "remove_object") and bucket:
                        mclient.remove_object(bucket, object_name)
                    logger.info("Cleaned up uploaded object after audit failure")
                except Exception as del_err:
                    logger.error("Failed to delete object after audit failure: %s", del_err)
                
                # MANDATORY: Raise error to fail the job
                raise RuntimeError(f"CRITICAL: Chunk {chunk_index} audit update failed - JOB MUST FAIL: {e}")

            if chunk_index % 10 == 0:
                # FIXED: Enhanced logging for historic loads
                if load_type == 'historic':
                    new_total = existing_total_records + current_delta_records
                    logger.info("FIXED: Delta %s - chunk %s completed (%s rows) | delta_total: %s | new_cumulative: %s", 
                               delta_column_value, chunk_index, recs, current_delta_records, new_total)
                else:
                    logger.info("FIXED: Chunk %s completed (%s rows) -> %s/%s", chunk_index, recs, bucket, object_name)
            chunk_index += 1
    
        # Finalize audit on success
        final_ist = datetime.now(IST)
        final_extraction_time = (final_ist - extraction_start).total_seconds()
        
        # FIXED: For historic loads, set final incremental values
        if load_type == 'historic':
            audit_log.update({
                "status": "COMPLETED",
                "task_endts": final_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": final_extraction_time,
                "extraction_time": final_extraction_time,
                "incremental_records": current_delta_records,  # Total records for this delta
                "incremental_extraction_time": final_extraction_time,  # Total time for this delta
                "incremental_apicalls": 0,  # Set based on actual API calls if any
            })
        else:
            audit_log.update({
                "status": "COMPLETED",
                "task_endts": final_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": final_extraction_time,
                "extraction_time": final_extraction_time,
                "total_records": current_delta_records,
            })
        
        # FIXED: Final audit update must succeed
        try:
            update_audit_record_strict(config_audit, audit_log)
            
            # FIXED: Enhanced completion logging
            if load_type == 'historic':
                new_total_records = existing_total_records + current_delta_records
                new_total_time = existing_extraction_time + final_extraction_time
                logger.info("FIXED: Completed delta %s processing - added %s records (%.2f sec) | NEW CUMULATIVE: %s records (%.2f sec)", 
                           delta_column_value, current_delta_records, final_extraction_time, 
                           new_total_records, new_total_time)
            else:
                logger.info("FIXED: Completed Oracle -> MinIO parquet for %s (load_type=%s)", table_name, load_type)
                
        except Exception as e:
            logger.error("CRITICAL: Final audit update failed - job completion not recorded: %s", e)
            raise RuntimeError(f"Final audit update failed: {e}")

        mclient.close()

    except Exception as e:
        final_ist = datetime.now(IST)
        final_extraction_time = (final_ist - extraction_start).total_seconds()
        
        if load_type == 'historic':
            audit_log.update({
                "status": "FAILED",
                "task_endts": final_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": final_extraction_time,
                "aerospike_error": str(e),
                "incremental_records": current_delta_records,  # Records processed before failure
                "incremental_extraction_time": final_extraction_time,
                "incremental_apicalls": 0,
            })
        else:
            audit_log.update({
                "status": "FAILED",
                "task_endts": final_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": final_extraction_time,
                "aerospike_error": str(e),
                "total_records": current_delta_records,
            })
            
        try:
            update_audit_record_strict(config_audit, audit_log)
        except Exception as audit_err:
            logger.error("Audit update after failure also failed: %s", audit_err)
        
        logger.error("CRITICAL: Oracle -> MinIO transfer failed: %s", e)
        raise RuntimeError(f"Extraction failed: {e}")
    finally:
        close_connection(cur, conn)

# Keep all other functions unchanged from your original code:
# - process_oracle_to_minio_with_dependencies
# - process_from_config  
# - main execution
# ... [REST OF YOUR CODE REMAINS THE SAME] ...

# -----------------------------------------------------------------------------
# Sequential Processing (unchanged)
# -----------------------------------------------------------------------------
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
    logger.info("FIXED: Begin processing Oracle->MinIO for %s up to %s (load_type=%s)",
               table_name_uc, current_business_loaddt, load_type)

    # Get jobs to process (ordered list of incomplete)
    dates = get_load_status_and_dates(
        config_audit, table_name_uc, current_business_loaddt, 
        load_type, delta_column, oracle_config if load_type == 'historic' else None
    )
    
    if not dates:
        logger.info("FIXED: No jobs to process - all completed or no jobs found.")
        return

    # Process each job sequentially (oldest first)
    for date_info in dates:
        biz_dt = date_info["business_loaddt"]
        status = date_info["status"]
        restart_point = int(date_info.get("restart_point") or 0)
        delta_value = date_info.get("delta_column_value")
        info_load_type = date_info.get("load_type", load_type)
        
        logger.info("FIXED: Processing job %s status=%s restart_point=%s delta_value=%s load_type=%s",
                   biz_dt, status, restart_point, delta_value, info_load_type)

        if status == "COMPLETED":
            logger.info("Job for %s already COMPLETED. Skipping.", biz_dt)
            continue

        if status == "RUNNING":
            logger.warning("Job for %s is RUNNING; treating as FAILED for restart.", biz_dt)
            status = "FAILED"

        if status in ("NOT_STARTED", "FAILED"):
            effective_restart = restart_point if status == "FAILED" else 0
            
            # Direct call to extraction
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
            
            logger.info("FIXED: Successfully completed processing for job %s delta_value=%s", biz_dt, delta_value)

    logger.info("FIXED: All required processing completed for %s", table_name_uc)

# -----------------------------------------------------------------------------
# Configuration-based processor (unchanged)
# -----------------------------------------------------------------------------
def process_from_config(config: Dict[str, Any], conn_config: Dict[str, Any],
                       current_business_loaddt: str) -> None:
    """Process all objects defined in configuration."""
    
    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})
    logger.info("FIXED: MinIO config loaded: %s", {k: v for k, v in minio_config.items() if k != 'secret_key'})
    
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
        load_type = obj_config.get("load_type", "delta")
        
        chunk_size = obj_config.get("chunksize", config.get("chunksize", 100_000))
        order_by = obj_config.get("ORDER_BY")
        delta_column = obj_config.get("delta_column") if load_type == 'historic' else None
        sub_folder = obj_config.get("sub_folder") if load_type == 'historic' else None
        
        logger.info("FIXED: Processing object: %s (load_type=%s)", table_name, load_type)
        
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
            logger.error("CRITICAL: Failed to process %s: %s", table_name, e)
            raise RuntimeError(f"Object processing failed for {table_name}: {e}")

# -----------------------------------------------------------------------------
# Main execution (unchanged)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    
    try:
        conn_config = get_system_config()
        
        config_yaml = "etl_configs/db2_to_uds_config.yml"
        with open(config_yaml, 'r', encoding="utf-8") as f:
            config_data = yaml.safe_load(f)["uds_to_minio"]
        
        current_date = datetime.now(IST).strftime("%Y-%m-%d")
        
        logger.info("FIXED: Starting configuration-based processing")
        process_from_config(config_data, conn_config, current_date)
        logger.info("FIXED: Processing completed successfully")
        
    except Exception as e:
        logger.error("CRITICAL: Main execution failed: %s", e)
        raise RuntimeError(f"Application failed: {e}")
