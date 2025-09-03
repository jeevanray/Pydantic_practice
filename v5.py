import os
import io
import json
import logging
import time
from typing import Dict, Any, List, Optional, Union
from typing import Sequence

import pandas as pd
import oracledb
import pytz
from datetime import datetime
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# External dependency expected by the caller's environment
from minio_handler import MinioHandler

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Constants & TZ
# -----------------------------------------------------------------------------
DATETIMEFORMAT = "%Y-%m-%d %H:%M:%S"
ORACLE_DATE_FMT = "YYYY-MM-DD"
IST = pytz.timezone("Asia/Kolkata")

# -----------------------------------------------------------------------------
# DB Helpers
# -----------------------------------------------------------------------------
def connect_to_oracle(oracle_conf: Dict[str, Any]) -> oracledb.Connection:
    """Connect to Oracle database with retry logic (3 attempts)."""
    logger.debug("Connecting to Oracle DB")
    attempt = 0
    last_err: Optional[Exception] = None
    while attempt < 3:
        try:
            return oracledb.connect(
                user=oracle_conf["user"],
                password=oracle_conf["password"],
                dsn=oracle_conf["dsn"],
            )
        except Exception as e:
            attempt += 1
            last_err = e
            logger.error("[Connection Attempt %s] Oracle connect failed: %s", attempt, str(e))
            time.sleep(5)
    assert last_err is not None
    raise last_err

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
# Schema Detection and Management
# -----------------------------------------------------------------------------
def detect_audit_schema_version(config_audit: Dict[str, Any]) -> Dict[str, bool]:
    """Detect which columns exist in the audit table for compatibility."""
    conn = connect_to_oracle(config_audit["target"])
    cur = conn.cursor()
    
    try:
        # Check for new columns
        column_check_query = """
            SELECT column_name 
            FROM all_tab_columns 
            WHERE table_name = UPPER(:tbl) 
              AND owner = UPPER(:own)
              AND column_name IN ('DELTA_COLUMN_VALUE', 'LOAD_TYPE')
        """
        
        cur.execute(column_check_query, {
            "tbl": config_audit["audit_table"], 
            "own": config_audit["schema"]
        })
        
        existing_columns = {row[0] for row in cur.fetchall()}
        
        return {
            "has_delta_column_value": "DELTA_COLUMN_VALUE" in existing_columns,
            "has_load_type": "LOAD_TYPE" in existing_columns,
            "is_legacy_schema": len(existing_columns) == 0
        }
        
    except Exception as e:
        logger.error(f"Failed to detect audit schema: {str(e)}")
        # Assume legacy schema if detection fails
        return {
            "has_delta_column_value": False,
            "has_load_type": False,
            "is_legacy_schema": True
        }
    finally:
        close_connection(cur, conn)

# -----------------------------------------------------------------------------
# Audit helpers
# -----------------------------------------------------------------------------
def prepare_auditing() -> Dict[str, Any]:
    """Base audit log dictionary with all expected keys present."""
    return {
        "source_table": "",
        "task_startts": "",
        "task_endts": "",
        "task_exec_secs": 0,
        "business_loaddt": "",
        "delta_column_value": None,  # Can be None for backward compatibility
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
        "load_type": "delta",  # Default to delta for backward compatibility
        "created_at_ts": None,
        "updated_at_ts": None,
    }

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def initialize_restart_audit_log(config_audit: Dict[str, Any], audit_log: Dict[str, Any], aud_dt: str, 
                                delta_column_value: Optional[str] = None) -> None:
    """Load prior audit row with schema compatibility - FIXED to avoid duplicates."""
    schema_info = detect_audit_schema_version(config_audit)
    
    # Build query based on schema version
    base_columns = """
        source_table, task_startts, task_endts, task_exec_secs, business_loaddt,
        total_records, extraction_time, total_apicalls, success_apicalls, failed_apicalls,
        api_failedpath, apicall_time, cdp_db_count_validation, aerospike_init_record_cnt,
        aerospike_init_read_waittime, aerospike_record_cnt, aerospike_waittime, aerospike_error,
        mongodb_init_record_cnt, mongodb_record_cnt, mongodb_init_read_waittime,
        mongodb_waittime, mongodb_error, suspected_updates_or_blacklisted_records,
        difference_aero_mongo, status, log_path, minio_filepath, restart_point
    """
    
    if schema_info["has_delta_column_value"] and schema_info["has_load_type"]:
        # New schema with proper unique key matching
        columns = base_columns + ", delta_column_value, load_type"
        where_clause = """
            BUSINESS_LOADDT = TO_DATE(:aud_dt, :fmt) 
            AND SOURCE_TABLE = :src
            AND NVL(DELTA_COLUMN_VALUE, 'NULL') = NVL(:delta_val, 'NULL')
            AND NVL(LOAD_TYPE, 'delta') = :load_type
        """
        params = {
            "aud_dt": aud_dt, 
            "fmt": ORACLE_DATE_FMT, 
            "src": audit_log["source_table"],
            "delta_val": delta_column_value,
            "load_type": audit_log.get("load_type", "delta")
        }
    else:
        # Legacy schema
        columns = base_columns + ", NULL as delta_column_value, 'delta' as load_type"
        where_clause = "BUSINESS_LOADDT = TO_DATE(:aud_dt, :fmt) AND SOURCE_TABLE = :src"
        params = {"aud_dt": aud_dt, "fmt": ORACLE_DATE_FMT, "src": audit_log["source_table"]}
    
    query = f"""
        SELECT {columns}
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE {where_clause}
    """
    
    conn = connect_to_oracle(config_audit["target"])
    cur = conn.cursor()
    try:
        logger.debug("Executing audit restart init query")
        cur.execute(query, params)
        row = cur.fetchone()
        if row:
            keys = [
                "source_table", "task_startts", "task_endts", "task_exec_secs", "business_loaddt",
                "total_records", "extraction_time", "total_apicalls", "success_apicalls", "failed_apicalls",
                "api_failedpath", "apicall_time", "cdp_db_count_validation", "aerospike_init_record_cnt",
                "aerospike_init_read_waittime", "aerospike_record_cnt", "aerospike_waittime", "aerospike_error",
                "mongodb_init_record_cnt", "mongodb_record_cnt", "mongodb_init_read_waittime",
                "mongodb_waittime", "mongodb_error", "suspected_updates_or_blacklisted_records",
                "difference_aero_mongo", "status", "log_path", "minio_filepath", "restart_point",
                "delta_column_value", "load_type"
            ]
            audit_log.update(dict(zip(keys, row)))
            
            # Handle CLOB fields
            try:
                from oracledb import LOB
                for field in ["api_failedpath", "aerospike_error", "mongodb_error"]:
                    if isinstance(audit_log.get(field), LOB):
                        audit_log[field] = audit_log[field].read()
            except Exception:
                pass
            
            logger.info("Audit restart initialized: status=%s restart_point=%s delta_value=%s", 
                       audit_log.get("status"), audit_log.get("restart_point"), 
                       audit_log.get("delta_column_value"))
        else:
            logger.info("No prior audit record; starting fresh")
    except Exception as e:
        logger.error("Failed to initialize audit record: %s", e)
        raise
    finally:
        close_connection(cur, conn)

def _ensure_time_fields(audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize types for time fields and booleans prior to MERGE."""
    # boolean -> 'Y'/'N'
    audit_data["cdp_db_count_validation"] = 'Y' if audit_data.get("cdp_db_count_validation") else 'N'

    # string -> datetime aware (IST) for known fields
    for k in ("task_startts", "task_endts"):
        v = audit_data.get(k)
        if isinstance(v, str) and v:
            audit_data[k] = IST.localize(datetime.strptime(v, DATETIMEFORMAT))
    
    v = audit_data.get("business_loaddt")
    if isinstance(v, str) and v:
        audit_data["business_loaddt"] = datetime.strptime(v, "%Y-%m-%d").date()

    # Handle None values for new columns - CRITICAL FIX
    if audit_data.get("delta_column_value") is None:
        audit_data["delta_column_value"] = None  # Keep as None for proper NULL handling
    if audit_data.get("load_type") is None:
        audit_data["load_type"] = "delta"

    # timestamps
    now_ist = datetime.now(IST)
    audit_data["updated_at_ts"] = now_ist
    if not audit_data.get("created_at_ts"):
        audit_data["created_at_ts"] = now_ist
    return audit_data

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def update_audit_record(config_audit: Dict[str, Any], audit_data: Dict[str, Any]) -> None:
    """FIXED: Upsert audit row with proper unique key matching to avoid duplicates."""
    audit_data = _ensure_time_fields(audit_data)
    schema_info = detect_audit_schema_version(config_audit)
    
    if schema_info["is_legacy_schema"]:
        # Legacy schema - use original merge logic
        merge_sql = f"""
            MERGE INTO {config_audit['schema']}.{config_audit['audit_table']} target
            USING (
                SELECT :source_table AS source_table, :business_loaddt AS business_loaddt FROM dual
            ) src
            ON (target.source_table = src.source_table AND target.business_loaddt = src.business_loaddt)
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
                source_table, business_loaddt, task_startts, task_endts, task_exec_secs,
                total_records, extraction_time, total_apicalls, success_apicalls, failed_apicalls,
                api_failedpath, apicall_time, cdp_db_count_validation,
                aerospike_init_record_cnt, aerospike_init_read_waittime,
                aerospike_record_cnt, aerospike_waittime, aerospike_error,
                mongodb_init_record_cnt, mongodb_record_cnt, mongodb_init_read_waittime,
                mongodb_waittime, mongodb_error, suspected_updates_or_blacklisted_records,
                difference_aero_mongo, status, log_path, minio_filepath, restart_point,
                created_at_ts, updated_at_ts
            ) VALUES (
                :source_table, :business_loaddt, :task_startts, :task_endts, :task_exec_secs,
                :total_records, :extraction_time, :total_apicalls, :success_apicalls, :failed_apicalls,
                :api_failedpath, :apicall_time, :cdp_db_count_validation,
                :aerospike_init_record_cnt, :aerospike_init_read_waittime,
                :aerospike_record_cnt, :aerospike_waittime, :aerospike_error,
                :mongodb_init_record_cnt, :mongodb_record_cnt, :mongodb_init_read_waittime,
                :mongodb_waittime, :mongodb_error, :suspected_updates_or_blacklisted_records,
                :difference_aero_mongo, :status, :log_path, :minio_filepath, :restart_point,
                :created_at_ts, :updated_at_ts
            )
        """
    else:
        # FIXED: New schema with proper NULL handling in ON clause
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

    conn = connect_to_oracle(config_audit["target"])
    cur = conn.cursor()
    try:
        logger.debug(
            "Upserting audit for table=%s loaddt=%s delta_value=%s status=%s",
            audit_data.get("source_table"), audit_data.get("business_loaddt"), 
            audit_data.get("delta_column_value"), audit_data.get("status")
        )
        cur.execute(merge_sql, audit_data)
        conn.commit()
        logger.debug("Audit upsert completed successfully")
    except Exception as e:
        conn.rollback()
        logger.error("Audit upsert failed: %s", e)
        raise
    finally:
        close_connection(cur, conn)

# -----------------------------------------------------------------------------
# FIXED: MinIO upload helper with proper client validation
# -----------------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def _upload_df_parquet(minio_client: MinioHandler, df: pd.DataFrame, object_path: str, compression: str = "snappy") -> None:
    """FIXED: Upload DataFrame as parquet to MinIO with proper client validation."""
    
    # CRITICAL FIX: Validate MinIO client before using
    if minio_client is None:
        raise ValueError("MinioHandler client is None - connection failed")
    
    # Check if client has required methods
    if not hasattr(minio_client, 'upload_dataframe') and not hasattr(minio_client, 'put_object'):
        raise ValueError("MinioHandler client missing required methods")
    
    try:
        # Try the enhanced upload method first
        if hasattr(minio_client, 'upload_dataframe') and callable(getattr(minio_client, 'upload_dataframe')):
            logger.debug(f"Using upload_dataframe method for {object_path}")
            return minio_client.upload_dataframe(df=df, object_path=object_path, format="parquet", compression=compression)
        
        # Fallback to manual parquet upload
        logger.debug(f"Using manual parquet upload for {object_path}")
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:
            raise RuntimeError("pyarrow is required for parquet upload fallback") from e
        
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=compression)
        data = buf.getvalue()
        
        # Validate put_object method exists and is callable
        if hasattr(minio_client, "put_object") and callable(getattr(minio_client, "put_object")):
            return minio_client.put_object(object_path, data, len(data), content_type="application/octet-stream")
        else:
            raise RuntimeError("MinioHandler client is None or missing put_object method")
            
    except Exception as e:
        logger.error(f"Failed to upload DataFrame to {object_path}: {str(e)}")
        raise

# -----------------------------------------------------------------------------
# FIXED: Load Status Functions
# -----------------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def get_load_status_and_dates(config_audit: Dict[str, Any], source_table: str, 
                             current_business_loaddt: str, load_type: str = 'delta',
                             delta_column: Optional[str] = None, 
                             oracle_config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    FIXED: Returns list of dicts for processing based on load_type with proper deduplication.
    """
    if load_type == 'historic' and delta_column and oracle_config:
        return get_historic_load_status(config_audit, source_table, current_business_loaddt, delta_column, oracle_config)
    else:
        return get_delta_load_status(config_audit, source_table, current_business_loaddt)

def get_delta_load_status(config_audit: Dict[str, Any], source_table: str, 
                         current_business_loaddt: str) -> List[Dict[str, Any]]:
    """FIXED: Get delta load status with proper schema compatibility and deduplication."""
    schema_info = detect_audit_schema_version(config_audit)
    
    if schema_info["is_legacy_schema"]:
        # Legacy schema query - only one record per source_table + business_loaddt
        query = f"""
            SELECT
                business_loaddt,
                NVL(status, 'NOT_STARTED') AS status,
                NVL(restart_point, 0) AS restart_point,
                NVL(total_records, 0) AS total_records,
                NULL as delta_column_value,
                'delta' as load_type
            FROM {config_audit["schema"]}.{config_audit["audit_table"]}
            WHERE business_loaddt <= TO_DATE(:current_dt, :fmt)
              AND source_table = :src
              AND NVL(status, 'NOT_STARTED') IN ('NOT_STARTED', 'FAILED', 'RUNNING')
            ORDER BY business_loaddt
        """
    else:
        # New schema query - ensure only delta load_type records
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

    conn = None
    cur = None
    try:
        conn = connect_to_oracle(config_audit["target"])
        cur = conn.cursor()
        cur.execute(query, {"current_dt": current_business_loaddt, "fmt": ORACLE_DATE_FMT, "src": source_table})
        rows = cur.fetchall()
        processed: List[Dict[str, Any]] = []
        for business_dt, status, restart_point, total_records, delta_val, load_type in rows:
            if hasattr(business_dt, "strftime"):
                biz_str = business_dt.strftime("%Y-%m-%d")
            else:
                biz_str = str(business_dt)
            processed.append({
                "business_loaddt": biz_str,
                "status": status,
                "restart_point": int(restart_point or 0),
                "total_records": int(total_records or 0),
                "delta_column_value": delta_val,
                "load_type": load_type
            })
        
        if not processed:
            logger.info("No audit rows found needing work for %s. Seeding current date as NOT_STARTED.", source_table)
            processed.append({
                "business_loaddt": current_business_loaddt,
                "status": "NOT_STARTED",
                "restart_point": 0,
                "total_records": 0,
                "delta_column_value": None,
                "load_type": 'delta'
            })
        return processed
    except Exception as e:
        logger.error("Failed to get delta load status: %s", e, exc_info=True)
        raise
    finally:
        close_connection(cur, conn)

def get_historic_load_status(config_audit: Dict[str, Any], source_table: str, 
                           current_business_loaddt: str, delta_column: str, 
                           oracle_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """FIXED: Get historic load status - process one delta value at a time sequentially."""
    
    # Query source table for next unprocessed delta value
    next_delta_query = f"""
        WITH source_deltas AS (
            SELECT DISTINCT TO_CHAR({delta_column}, 'YYYY-MM-DD') as delta_value
            FROM {source_table}
            WHERE {delta_column} IS NOT NULL
        ),
        processed_deltas AS (
            SELECT delta_column_value as delta_value
            FROM {config_audit["schema"]}.{config_audit["audit_table"]}
            WHERE source_table = :src
              AND load_type = 'historic'
              AND status = 'COMPLETED'
              AND delta_column_value IS NOT NULL
        )
        SELECT MIN(sd.delta_value) as next_delta
        FROM source_deltas sd
        WHERE sd.delta_value NOT IN (SELECT delta_value FROM processed_deltas)
        ORDER BY sd.delta_value
    """
    
    source_conn = None
    audit_conn = None
    source_cur = None
    audit_cur = None
    try:
        # Connect to source database to get next delta value
        source_conn = connect_to_oracle(oracle_config)
        source_cur = source_conn.cursor()
        
        source_cur.execute(next_delta_query, {"src": source_table})
        result = source_cur.fetchone()
        next_delta = result[0] if result and result[0] else None
        
        if not next_delta:
            logger.info("No more historic data to process for %s", source_table)
            return []
        
        # Check audit status for this specific delta value
        status_query = f"""
            SELECT
                business_loaddt,
                NVL(status, 'NOT_STARTED') AS status,
                NVL(restart_point, 0) AS restart_point,
                NVL(total_records, 0) AS total_records,
                delta_column_value,
                NVL(load_type, 'historic') AS load_type
            FROM {config_audit["schema"]}.{config_audit["audit_table"]}
            WHERE source_table = :src
              AND load_type = 'historic'
              AND delta_column_value = :delta_val
              AND business_loaddt = TO_DATE(:current_dt, :fmt)
        """
        
        audit_conn = connect_to_oracle(config_audit["target"])
        audit_cur = audit_conn.cursor()
        
        audit_cur.execute(status_query, {
            "src": source_table, 
            "delta_val": next_delta, 
            "current_dt": current_business_loaddt,
            "fmt": ORACLE_DATE_FMT
        })
        
        row = audit_cur.fetchone()
        if row:
            business_dt, status, restart_point, total_records, delta_val, load_type = row
            if hasattr(business_dt, "strftime"):
                biz_str = business_dt.strftime("%Y-%m-%d")
            else:
                biz_str = str(business_dt)
                
            return [{
                "business_loaddt": biz_str,
                "status": status,
                "restart_point": int(restart_point or 0),
                "total_records": int(total_records or 0),
                "delta_column_value": next_delta,
                "load_type": 'historic'
            }]
        else:
            # Create new entry for this delta value
            return [{
                "business_loaddt": current_business_loaddt,
                "status": "NOT_STARTED",
                "restart_point": 0,
                "total_records": 0,
                "delta_column_value": next_delta,
                "load_type": 'historic'
            }]
            
    except Exception as e:
        logger.error("Failed to get historic load status: %s", e, exc_info=True)
        raise
    finally:
        if source_cur:
            source_cur.close()
        if source_conn:
            source_conn.close()
        if audit_cur:
            audit_cur.close()
        if audit_conn:
            audit_conn.close()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def create_audit_table_if_not_exists(config_audit: Dict[str, Any]) -> None:
    """FIXED: Create or update audit table with proper constraints to prevent duplicates."""
    check_query = """
        SELECT COUNT(*)
        FROM all_tables
        WHERE table_name = UPPER(:tbl)
          AND owner = UPPER(:own)
    """

    conn = connect_to_oracle(config_audit["target"])
    cur = conn.cursor()
    try:
        cur.execute(check_query, {"tbl": config_audit["audit_table"], "own": config_audit["schema"]})
        exists = cur.fetchone()[0]
        if exists == 0:
            logger.warning("Audit table '%s' not found. Creating with full schema...", config_audit["audit_table"])
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
                    CONSTRAINT uk_audit_unique UNIQUE (source_table, business_loaddt, NVL(delta_column_value, 'NULL'), load_type)
                )
            """
            cur.execute(create_sql)
            
            # Create performance index
            cur.execute(
                f"CREATE INDEX idx_{config_audit['audit_table']}_perf ON {config_audit['schema']}.{config_audit['audit_table']}(source_table, business_loaddt, load_type, status)"
            )
            
            conn.commit()
            logger.info("Audit table created with proper constraints.")
        else:
            logger.info("Audit table exists. Checking schema compatibility...")
            schema_info = detect_audit_schema_version(config_audit)
            
            if schema_info["is_legacy_schema"]:
                logger.info("Legacy audit table detected. Adding new columns for historic support...")
                try:
                    if not schema_info["has_delta_column_value"]:
                        cur.execute(f"ALTER TABLE {config_audit['schema']}.{config_audit['audit_table']} ADD (delta_column_value VARCHAR2(100))")
                        logger.info("Added DELTA_COLUMN_VALUE column")
                        
                    if not schema_info["has_load_type"]:
                        cur.execute(f"ALTER TABLE {config_audit['schema']}.{config_audit['audit_table']} ADD (load_type VARCHAR2(20) DEFAULT 'delta')")
                        logger.info("Added LOAD_TYPE column")
                        
                    # Update existing records to have default load_type
                    cur.execute(f"UPDATE {config_audit['schema']}.{config_audit['audit_table']} SET load_type = 'delta' WHERE load_type IS NULL")
                    
                    conn.commit()
                    logger.info("Successfully upgraded audit table schema for historic support")
                    
                except Exception as alter_error:
                    logger.warning(f"Could not upgrade audit table schema: {alter_error}. Will use legacy compatibility mode.")
                    conn.rollback()
            else:
                logger.info("Modern audit table schema detected. Ready for historic loads.")
                
    except Exception as e:
        logger.error("Failed to create/update audit table: %s", e)
        raise
    finally:
        close_connection(cur, conn)

def _update_audit_with_retry(config_audit: Dict[str, Any], audit_log: Dict[str, Any], retries: int = 3, backoff_seconds: int = 2) -> None:
    """Try persisting the audit_log up to `retries` times with exponential backoff."""
    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt < retries:
        try:
            update_audit_record(config_audit, audit_log)
            return
        except Exception as e:
            last_exc = e
            attempt += 1
            logger.warning("Audit persist attempt %s/%s failed: %s", attempt, retries, e)
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))
    assert last_exc is not None
    raise last_exc

# -----------------------------------------------------------------------------
# Path Generation Helper
# -----------------------------------------------------------------------------
def generate_object_path(base_path: str, business_loaddt: str, load_type: str, 
                        delta_column_value: Optional[str] = None, sub_folder: Optional[str] = None) -> str:
    """Generate object path based on load type and configuration."""
    try:
        load_dt = datetime.strptime(business_loaddt, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"business_loaddt must be YYYY-MM-DD, got {business_loaddt}") from e
    
    date_folder = load_dt.strftime("%d%m%Y")
    
    if load_type == 'historic':
        if delta_column_value:
            return f"{base_path}/history/{delta_column_value}"
        else:
            raise ValueError("delta_column_value is required for historic load type")
    else:
        # For delta: base_path/delta/date_folder
        return f"{base_path}/delta/{date_folder}"

# -----------------------------------------------------------------------------
# FIXED: Core Extraction Function
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
    FIXED: Enhanced extraction with proper MinIO client validation and audit handling.
    """
    extraction_start = datetime.now(IST)
    bucket = minio_config.get("bucket_name", "sbi-test")

    # Generate effective object path based on load type
    if load_type == 'historic':
        effective_object_path = generate_object_path(
            base_object_path, business_loaddt, load_type, delta_column_value, sub_folder
        )
    else:
        # Original delta behavior
        load_dt = datetime.strptime(business_loaddt, "%Y-%m-%d")
        date_folder = load_dt.strftime("%d%m%Y")
        effective_object_path = f"{base_object_path}/{date_folder}"

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

    # Pull any previous audit state for restart
    initialize_restart_audit_log(config_audit, audit_log, business_loaddt, delta_column_value)

    # Use higher of provided restart_point vs audit restart_point
    start_chunk_index = max(int(restart_point or 0), int(audit_log.get("restart_point") or 0))
    total_records = int(audit_log.get("total_records") or 0)

    logger.info("Starting extraction for %s on %s (load_type=%s, delta_value=%s)", 
               table_name, business_loaddt, load_type, delta_column_value)
    logger.info("Restart chunk index: %s | total_records so far: %s", start_chunk_index, total_records)

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
    mclient = None

    try:
        # Connection + FIXED MinIO client initialization
        conn = connect_to_oracle(oracle_config)
        
        # CRITICAL FIX: Properly initialize MinIO client and validate
        try:
            mclient = MinioHandler(minio_config)
            if mclient is None:
                raise ValueError("MinioHandler initialization returned None")
            logger.info("MinIO client initialized successfully")
        except Exception as minio_error:
            logger.error(f"Failed to initialize MinIO client: {minio_error}")
            raise
        
        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))
        logger.info("Executing SELECT for streaming: %s", select_sql)
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
                logger.info("No more data to process for %s", table_name)
                break

            df_chunk = pd.DataFrame(rows, columns=cols)
            
            # Generate object name based on load type
            if load_type == 'historic' and delta_column_value:
                object_name = f"{effective_object_path}/{table_name.replace('.', '_')}_{delta_column_value}_{chunk_index:06d}.parquet"
            else:
                object_name = f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"

            # FIXED: Upload parquet chunk to MinIO with proper validation
            try:
                _upload_df_parquet(mclient, df_chunk, object_name, compression=compression)
                recs = len(df_chunk)
                logger.info("Successfully uploaded chunk %s (%s rows) -> %s/%s", chunk_index, recs, bucket, object_name)
            except Exception as upload_error:
                logger.error(f"Failed to upload chunk {chunk_index}: {upload_error}")
                raise

            # Prepare audit update
            now_ist = datetime.now(IST)
            proposed_total = total_records + recs
            audit_log.update({
                "total_records": proposed_total,
                "status": "RUNNING",
                "task_endts": now_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": (now_ist - extraction_start).total_seconds(),
                "extraction_time": (now_ist - extraction_start).total_seconds(),
                "restart_point": chunk_index + 1,
                "minio_filepath": effective_object_path,
            })

            try:
                _update_audit_with_retry(config_audit, audit_log, retries=3)
                logger.debug("Audit updated successfully for chunk %s", chunk_index)
            except Exception as e:
                logger.error("Audit update failed after retries; deleting uploaded object: %s/%s", bucket, object_name)
                try:
                    if hasattr(mclient, "delete_file") and bucket:
                        mclient.delete_file(object_name, bucket)
                    elif hasattr(mclient, "remove_object") and bucket:
                        mclient.remove_object(bucket, object_name)
                except Exception as del_err:
                    logger.error("Failed to delete object after audit failure: %s", del_err)
                raise

            # Only update counters after successful audit persist
            total_records = proposed_total
            chunk_index += 1
    
        # Finalize audit on success
        final_ist = datetime.now(IST)
        audit_log.update({
            "status": "COMPLETED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": (final_ist - extraction_start).total_seconds(),
            "extraction_time": (final_ist - extraction_start).total_seconds(),
        })
        update_audit_record(config_audit, audit_log)
        logger.info("Completed Oracle -> MinIO parquet for %s (load_type=%s)", table_name, load_type)

    except Exception as e:
        final_ist = datetime.now(IST)
        audit_log.update({
            "status": "FAILED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": (final_ist - extraction_start).total_seconds(),
            "aerospike_error": str(e),
        })
        try:
            _update_audit_with_retry(config_audit, audit_log, retries=3)
        except Exception as audit_err:
            logger.error("Audit update after failure also failed: %s", audit_err)
        logger.error("Oracle -> MinIO transfer failed: %s", e)
        raise
    finally:
        # FIXED: Proper resource cleanup
        close_connection(cur, conn)
        if mclient and hasattr(mclient, 'close'):
            try:
                mclient.close()
            except Exception as close_err:
                logger.error("Error closing MinIO client: %s", close_err)

# -----------------------------------------------------------------------------
# Enhanced Orchestrator
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
    """
    FIXED: Enhanced driver with proper single-record processing for historic loads.
    """
    table_name_uc = table_name.upper()
    logger.info("Begin processing Oracle->MinIO for %s up to %s (load_type=%s)", 
               table_name_uc, current_business_loaddt, load_type)

    dates = get_load_status_and_dates(
        config_audit, table_name_uc, current_business_loaddt, 
        load_type, delta_column, oracle_config if load_type == 'historic' else None
    )
    if not dates:
        logger.info("Nothing to process.")
        return

    # FIXED: For historic loads, process only ONE delta value at a time
    if load_type == 'historic':
        logger.info("Historic load mode: processing one delta value at a time")
        if len(dates) > 1:
            logger.warning("Multiple historic dates found, will process only the first one")
            dates = dates[:1]  # Process only first delta value

    for date_info in dates:
        biz_dt = date_info["business_loaddt"]
        status = date_info["status"]
        restart_point = int(date_info.get("restart_point") or 0)
        delta_value = date_info.get("delta_column_value")
        info_load_type = date_info.get("load_type", load_type)
        
        logger.info("Processing %s status=%s restart_point=%s delta_value=%s load_type=%s", 
                   biz_dt, status, restart_point, delta_value, info_load_type)

        try:
            if status == "RUNNING":
                logger.warning("Date %s is RUNNING; treating as FAILED for restart.", biz_dt)
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
            elif status == "COMPLETED":
                logger.info("Date %s already COMPLETED. Skipping.", biz_dt)
                # For historic loads, if current delta is completed, next run will pick up next delta value
                
        except Exception as e:
            logger.error("Failed to process %s: %s", biz_dt, e)
            # Don't continue to next dates for historic loads on failure
            if load_type == 'historic':
                raise
            continue

    logger.info("All required processing completed for %s", table_name_uc)

# -----------------------------------------------------------------------------
# Configuration-based processor (unchanged)
# -----------------------------------------------------------------------------
def process_from_config(config: Dict[str, Any], conn_config: Dict[str, Any], 
                       current_business_loaddt: str) -> None:
    """Process all objects defined in configuration."""
    
    # Extract connection details
    oracle_config = conn_config.get("oracle", {})
    minio_config = conn_config.get("minio", {})
    
    # Setup audit configuration
    audit_config = {
        "target": oracle_config,
        "schema": config.get("schema", "uds"),
        "audit_table": config.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }
    
    # Process each object
    for obj_config in config.get("objects", []):
        if not obj_config.get("isactive", True):
            logger.info("Skipping inactive object: %s", obj_config.get("db_table"))
            continue
            
        table_name = f"{obj_config.get('schema', config.get('schema'))}.{obj_config['db_table']}"
        output_path = obj_config.get("output_path", config.get("output_path"))
        load_type = obj_config.get("load_type", "delta")
        
        # Get object-specific settings
        chunk_size = obj_config.get("chunksize", config.get("chunksize", 100_000))
        order_by = obj_config.get("ORDER_BY")
        delta_column = obj_config.get("delta_column") if load_type == 'historic' else None
        sub_folder = obj_config.get("sub_folder") if load_type == 'historic' else None
        
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
            continue

# -----------------------------------------------------------------------------
# Main execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    oracle_connection_config = {
        "user": os.environ.get("ORACLE_USER", "uds"),
        "password": os.environ.get("ORACLE_PASSWORD"),
        "dsn": os.environ.get("ORACLE_DSN")
    }

    minio_connection_config = {
        "endpoint": os.environ.get("MINIO_ENDPOINT"),
        "access_key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret_key": os.environ.get("MINIO_SECRET_KEY"),
        "bucket_name": os.environ.get("MINIO_BUCKET", "sbi-test"),
        "secure": os.environ.get("MINIO_SECURE", "true").lower() == "true"
    }

    audit_config = {
        "target": oracle_connection_config,
        "schema": os.environ.get("AUDIT_SCHEMA", "UDS"),
        "audit_table": os.environ.get("AUDIT_TABLE", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }

    table_to_extract = os.environ.get("ORACLE_TABLE", "CDP_UNICA_REFINED.CUSTOMER_PROFILE")
    minio_output_path = os.environ.get("MINIO_OUTPUT_BASE", "segmentation/segmentdb_6550/input_data/customer_data/customer_profile")
    current_business_load_date = os.environ.get("BUSINESS_LOAD_DATE", datetime.now(IST).strftime("%Y-%m-%d"))
    order_by_col = os.environ.get("ORDER_BY")

    logger.info("Starting process_oracle_to_minio_with_dependencies")
    process_oracle_to_minio_with_dependencies(
        oracle_config=oracle_connection_config,
        minio_config=minio_connection_config,
        config_audit=audit_config,
        table_name=table_to_extract,
        base_object_path=minio_output_path,
        current_business_loaddt=current_business_load_date,
        chunk_size=int(os.environ.get("CHUNK_SIZE", "100000")),
        compression=os.environ.get("PARQUET_COMPRESSION", "snappy"),
        order_by=order_by_col,
        load_type="delta",  # Default to delta for backward compatibility
    )
