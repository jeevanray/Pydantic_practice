
import logging
import time
from typing import Dict, Any, List, Optional, Union, Sequence, Tuple
from decimal import Decimal
from datetime import datetime, date

import oracledb
from oracledb import LOB
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utilities import connect_to_oracle, _ensure_time_fields, close_connection
from constants import DATEFORMAT, DATETIMEFORMAT,ORACLE_DATE


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def prepare_auditing() -> Dict[str, Any]:
    """Base audit log dictionary with all expected keys present."""
    return {
        "source_table": "",
        "task_startts": None,
        "task_endts": None,
        "task_exec_secs": 0,
        "business_loaddt": None,
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
    """Load existing audit record for restart."""
    # Defensive: ensure business_loaddt is set for delta loads so downstream MERGE
    # operations don't insert a row with NULL business_loaddt.
    if audit_log.get("load_type") != "historic":
        # prefer existing value, otherwise use the supplied aud_dt
        if not audit_log.get("business_loaddt"):
            audit_log["business_loaddt"] = aud_dt
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
            "fmt": ORACLE_DATE,
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
def update_audit_record_strict(config_audit: Dict[str, Any], audit_data: Dict[str, Any],
                              max_attempts: int = 5, wait_seconds: int = 3) -> None:
    """Strict audit update with proper composite key matching."""
    audit_data_copy = _ensure_time_fields(audit_data)

    # logger.info(f"audit_data_copy: {audit_data_copy} and audit_data: {audit_data}")

    # For historic loads, use delta_column_value as primary key component
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
    else:
        # Original delta load merge logic - ensure business_loaddt is bound as a DATE using TO_DATE
        merge_sql = f"""
            MERGE INTO {config_audit['schema']}.{config_audit['audit_table']} target
            USING (
                SELECT
                    :source_table AS source_table,
                    :business_loaddt as business_loaddt,
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
            # Ensure date format binding is present when using TO_DATE in MERGE
            conn = connect_to_oracle(config_audit["target"])
            conn.autocommit = False
            cur = conn.cursor()

            logger.debug(
                "Audit update (attempt %s/%s) for table=%s delta_value=%s status=%s",
                attempt + 1, max_attempts, audit_data.get("source_table"),
                audit_data.get("delta_column_value"), audit_data.get("status")
            )

            cur.execute(merge_sql, audit_data_copy)
            conn.commit()

            logger.info("Audit update successful on attempt %s", attempt + 1)
            return
        except Exception as e:
            last_error = e
            logger.error("Audit update failed on attempt %s/%s: %s", attempt + 1, max_attempts, e)
            logger.error("e")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if attempt < max_attempts - 1:
                logger.warning("Waiting %s seconds before retry...", wait_seconds)
                time.sleep(wait_seconds)
                raise e
        finally:
            close_connection(cur, conn)

    # All attempts failed
    error_msg = f"CRITICAL: Audit update failed after {max_attempts} attempts"
    logger.error(error_msg)
    if last_error:
        raise RuntimeError(f"{error_msg}: {last_error}")
    else:
        raise RuntimeError(error_msg)
    
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

                    # Create performance indexes                                                                        ## Mj - not sure why index were created - as the primary key is already set on run_id
                    # cur.execute(
                    #     f"CREATE INDEX idx_{config_audit['audit_table']}_status ON {config_audit['schema']}.{config_audit['audit_table']}(source_table, status, load_type)"
                    # )
                    # cur.execute(
                    #     f"CREATE INDEX idx_{config_audit['audit_table']}_loaddt ON {config_audit['schema']}.{config_audit['audit_table']}(business_loaddt, load_type)"
                    # )

                    conn.commit()
                    logger.info("Audit table created with proper constraints.")

                else:                                                                                                  ## Mj - Too many complicated nested blocks
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