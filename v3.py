import os
import io
import logging
import time
from typing import Dict, Any, List, Optional
from typing import Sequence

import pandas as pd
import oracledb
import pytz
from datetime import datetime
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# External dependency expected by the caller's environment
# Must provide a safe, public API. We will try a graceful fallback if some methods are absent.
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
        except Exception as e:  # noqa: BLE001 (broad ok for retries)
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
        logger.info("Closed Oracle connection")
    except Exception as e:  # noqa: BLE001
        logger.error("Error closing Oracle connection: %s", e)

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
        "aerospike_error": None,  # kept for compatibility (generic error field)
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
        # timestamps managed by code
        "created_at_ts": None,
        "updated_at_ts": None,
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def initialize_restart_audit_log(config_audit: Dict[str, Any], audit_log: Dict[str, Any], aud_dt: str) -> None:
    """Load prior audit row (if any) and merge into audit_log using bind variables."""
    query = f"""
        SELECT
            source_table,
            task_startts,
            task_endts,
            task_exec_secs,
            business_loaddt,
            total_records,
            extraction_time,
            total_apicalls,
            success_apicalls,
            failed_apicalls,
            api_failedpath,
            apicall_time,
            cdp_db_count_validation,
            aerospike_init_record_cnt,
            aerospike_init_read_waittime,
            aerospike_record_cnt,
            aerospike_waittime,
            aerospike_error,
            mongodb_init_record_cnt,
            mongodb_record_cnt,
            mongodb_init_read_waittime,
            mongodb_waittime,
            mongodb_error,
            suspected_updates_or_blacklisted_records,
            difference_aero_mongo,
            status,
            log_path,
            minio_filepath,
            restart_point
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE BUSINESS_LOADDT = TO_DATE(:aud_dt, :fmt)
          AND SOURCE_TABLE = :src
    """
    conn = connect_to_oracle(config_audit["target"])
    cur = conn.cursor()
    try:
        logger.debug("Executing audit restart init query")
        cur.execute(query, {"aud_dt": aud_dt, "fmt": ORACLE_DATE_FMT, "src": audit_log["source_table"]})
        row = cur.fetchone()
        if row:
            keys = list(audit_log.keys())
            # Only take up to the number of columns returned
            audit_log.update(dict(zip(keys, row)))
            # Ensure CLOB is read as string
            try:
                from oracledb import LOB  # type: ignore
                if isinstance(audit_log.get("api_failedpath"), LOB):
                    audit_log["api_failedpath"] = audit_log["api_failedpath"].read()
            except Exception:  # noqa: BLE001
                pass
            logger.info("Audit restart initialized: status=%s restart_point=%s", audit_log.get("status"), audit_log.get("restart_point"))
        else:
            logger.info("No prior audit record; starting fresh")
    except Exception as e:  # noqa: BLE001
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

    # timestamps
    now_ist = datetime.now(IST)
    audit_data["updated_at_ts"] = now_ist
    if not audit_data.get("created_at_ts"):
        audit_data["created_at_ts"] = now_ist
    return audit_data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def update_audit_record(config_audit: Dict[str, Any], audit_data: Dict[str, Any]) -> None:
    """Upsert audit row by (source_table, business_loaddt) using bind variables."""
    audit_data = _ensure_time_fields(audit_data)

    merge_sql = f"""
        MERGE /*+ PARALLEL(target, 4) */ INTO {config_audit['schema']}.{config_audit['audit_table']} target
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

    conn = connect_to_oracle(config_audit["target"])
    cur = conn.cursor()
    try:
        logger.debug(
            "Upserting audit for table=%s loaddt=%s status=%s",
            audit_data.get("source_table"), audit_data.get("business_loaddt"), audit_data.get("status")
        )
        cur.execute(merge_sql, audit_data)
        conn.commit()
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        logger.error("Audit upsert failed: %s", e)
        raise
    finally:
        close_connection(cur, conn)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def get_load_status_and_dates(config_audit: Dict[str, Any], source_table: str, current_business_loaddt: str) -> List[Dict[str, Any]]:
    """
    Returns list of dicts with keys: business_loaddt (YYYY-MM-DD str), status, restart_point, total_records.
    NOTE: This only returns dates already present in the audit table. If you want to drive missing dates,
    seed the table externally or extend this function to generate a date series.
    """
    query = f"""
        SELECT
            business_loaddt,
            NVL(status, 'NOT_STARTED') AS status,
            NVL(restart_point, 0) AS restart_point,
            NVL(total_records, 0) AS total_records
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE business_loaddt <= TO_DATE(:current_dt, :fmt)
          AND source_table = :src
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
        for business_dt, status, restart_point, total_records in rows:
            # Normalize business date to string
            if hasattr(business_dt, "strftime"):
                biz_str = business_dt.strftime("%Y-%m-%d")
            else:
                biz_str = str(business_dt)
            processed.append({
                "business_loaddt": biz_str,
                "status": status,
                "restart_point": int(restart_point or 0),
                "total_records": int(total_records or 0),
            })
        if not processed:
            logger.info("No audit rows found needing work for %s. Seeding current date as NOT_STARTED.", source_table)
            processed.append({
                "business_loaddt": current_business_loaddt,
                "status": "NOT_STARTED",
                "restart_point": 0,
                "total_records": 0,
            })
        return processed
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to get load status and dates: %s", e, exc_info=True)
        raise
    finally:
        close_connection(cur, conn)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def create_audit_table_if_not_exists(config_audit: Dict[str, Any]) -> None:
    """Create audit table if missing. Uses unquoted uppercase identifiers for consistency."""
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
            logger.warning("Audit table '%s' not found. Creating...", config_audit["audit_table"])
            create_sql = f"""
                CREATE TABLE {config_audit['schema']}.{config_audit['audit_table']} (
                    run_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    source_table VARCHAR2(100),
                    task_startts TIMESTAMP,
                    task_endts TIMESTAMP,
                    task_exec_secs NUMBER,
                    business_loaddt DATE,
                    total_records NUMBER,
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
                    created_at_ts TIMESTAMP,
                    updated_at_ts TIMESTAMP,
                    CONSTRAINT uq_source_bizdate UNIQUE (source_table, business_loaddt)
                )
            """
            cur.execute(create_sql)
            # Helpful index for merges/reads
            cur.execute(
                f"CREATE INDEX IDX_{config_audit['audit_table']}_SRC_BIZ ON {config_audit['schema']}.{config_audit['audit_table']}(source_table, business_loaddt)"
            )
            conn.commit()
            logger.info("Audit table created.")
        else:
            logger.info("Audit table already exists.")
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to create audit table: %s", e)
        raise
    finally:
        close_connection(cur, conn)


def _update_audit_with_retry(config_audit: Dict[str, Any], audit_log: Dict[str, Any], retries: int = 3, backoff_seconds: int = 2) -> None:
    """Try persisting the audit_log up to `retries` times with exponential backoff.

    Raises the last exception if all attempts fail.
    """
    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt < retries:
        try:
            update_audit_record(config_audit, audit_log)
            return
        except Exception as e:  # noqa: BLE001
            last_exc = e
            attempt += 1
            logger.warning("Audit persist attempt %s/%s failed: %s", attempt, retries, e)
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))
    assert last_exc is not None
    raise last_exc

# -----------------------------------------------------------------------------
# MinIO upload helper
# -----------------------------------------------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(Exception))
def _upload_df_parquet(minio_client: MinioHandler, df: pd.DataFrame, object_path: str, compression: str = "snappy") -> None:
    """Try a public helper on MinioHandler; fallback to writing bytes and putting object.
    Your MinioHandler should expose either `upload_dataframe(df, object_path, format, compression)`
    or a lower-level `put_object(object_name, data_bytes_or_stream, length, content_type)`.
    """
    # Try to use a handler-provided upload_dataframe; accept a few common signatures.
    upload_fn = getattr(minio_client, "upload_dataframe", None)
    if upload_fn:
        try:
            return upload_fn(df=df, object_path=object_path, format="parquet", compression=compression)  # type: ignore[attr-defined]
        except TypeError:
            # Some handlers expose (bucket_name, object_name, df, format)
            bucket = getattr(minio_client, "bucket_name", None)
            if bucket is not None:
                try:
                    return upload_fn(bucket, object_path, df, "parquet")  # type: ignore[arg-type]
                except Exception:
                    # fallback to byte upload below
                    pass

    # Fallback to manual parquet -> bytes and use put_object/remove API
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception as e:  # noqa: BLE001
        raise RuntimeError("pyarrow is required for parquet upload fallback") from e

    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)
    buf.seek(0)

    # Prefer MinIO client's put_object(bucket, object_name, data_stream, length)
    if hasattr(minio_client, "put_object"):
        bucket = getattr(minio_client, "bucket_name", None)
        if bucket is not None:
            # put_object typically accepts a file-like or bytes and length
            minio_client.put_object(bucket, object_path, buf, buf.getbuffer().nbytes)  # type: ignore[attr-defined]
            return
        # Some handlers implement put_object(object_name, data, length)
        try:
            data = buf.getvalue()
            minio_client.put_object(object_path, data, len(data))  # type: ignore[attr-defined]
            return
        except Exception:
            pass

    raise RuntimeError("MinioHandler must provide upload_dataframe(...) or put_object(...)")

# -----------------------------------------------------------------------------
# Core Extraction
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
    minio_handler: Optional['MinioHandler'] = None
) -> None:
    """
    Extract rows from Oracle table and upload to MinIO as parquet in chunks with restartability.

    Restart strategy: stream with deterministic ORDER BY; fast-forward by discarding `restart_point` chunks,
    then continue emitting chunks and updating `restart_point` after each successful upload.
    """
    extraction_start = datetime.now(IST)

    # Derive effective object path: base/<ddMMyyyy>
    try:
        load_dt = datetime.strptime(business_loaddt, "%Y-%m-%d")
    except ValueError as e:  # noqa: BLE001
        raise ValueError(f"business_loaddt must be YYYY-MM-DD, got {business_loaddt}") from e
    date_folder = load_dt.strftime("%d%m%Y")
    effective_object_path = f"{base_object_path}/{date_folder}"

    # Ensure audit table exists
    create_audit_table_if_not_exists(config_audit)

    # Initialize audit log
    audit_log = prepare_auditing()
    audit_log.update({
        "source_table": table_name.upper(),
        "business_loaddt": business_loaddt,
        "task_startts": extraction_start.strftime(DATETIMEFORMAT),
        "status": "RUNNING",
        "log_path": config_audit.get("output_file_path", base_object_path),
        "minio_filepath": effective_object_path,
    })

    # Pull any previous audit state for restart
    initialize_restart_audit_log(config_audit, audit_log, business_loaddt)

    # Use higher of provided restart_point vs audit restart_point
    start_chunk_index = max(int(restart_point or 0), int(audit_log.get("restart_point") or 0))
    total_records = int(audit_log.get("total_records") or 0)

    logger.info("Starting extraction for %s on %s", table_name, business_loaddt)
    logger.info("Restart chunk index: %s | total_records so far: %s", start_chunk_index, total_records)

    # Build SELECT with deterministic ordering
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT * FROM {table_name}{order_clause}"

    conn = None
    cur = None

    try:
        # Connection + MinIO client
        conn = connect_to_oracle(oracle_config)
        # Instantiate MinioHandler from config dict if provided
        if isinstance(minio_config, dict):
            mclient = MinioHandler(
                minio_config.get("endpoint") or "",
                minio_config.get("access_key") or "",
                minio_config.get("secret_key") or "",
                minio_config.get("secure", True),
                minio_config.get("verify_certs", True),
            )
            setattr(mclient, "bucket_name", minio_config.get("bucket_name"))
        else:
            mclient = minio_config  # assume already an instance

        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))  # sensible arraysize bounds
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
            object_name = f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"

            # upload parquet chunk to MinIO
            _upload_df_parquet(mclient, df_chunk, object_name, compression=compression)
            recs = len(df_chunk)

            # Prepare audit update but persist only after we confirm audit write succeeded.
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

            # Attempt to persist audit with retries. If audit persist fails, delete the uploaded object to avoid skipping.
            bucket = getattr(mclient, "bucket_name", None)
            try:
                _update_audit_with_retry(config_audit, audit_log, retries=3)
            except Exception as e:  # noqa: BLE001
                logger.error("Audit update failed after retries; deleting uploaded object to avoid skipping: %s/%s", bucket, object_name)
                # best-effort delete; don't suppress original error
                try:
                    if hasattr(mclient, "delete_object") and bucket:
                        mclient.delete_object(bucket, object_name)  # type: ignore[attr-defined]
                    elif hasattr(mclient, "remove_object") and bucket:
                        mclient.remove_object(bucket, object_name)  # type: ignore[attr-defined]
                except Exception as del_err:  # noqa: BLE001
                    logger.error("Failed to delete object after audit failure: %s", del_err)
                # re-raise so caller treats this as failure and can retry the chunk safely
                raise

            # Only update counters after successful audit persist
            total_records = proposed_total
            logger.info("Uploaded chunk %s (%s rows) -> %s/%s", chunk_index, recs, bucket, object_name)
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
            logger.info("Completed Oracle -> MinIO parquet for %s", table_name)

    except Exception as e:  # noqa: BLE001
        final_ist = datetime.now(IST)
        audit_log.update({
            "status": "FAILED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": (final_ist - extraction_start).total_seconds(),
            # keep field name for compatibility
            "aerospike_error": str(e),
        })
        try:
            _update_audit_with_retry(config_audit, audit_log, retries=3)
        except Exception as audit_err:  # noqa: BLE001
            logger.error("Audit update after failure also failed: %s", audit_err)
        logger.error("Oracle -> MinIO transfer failed: %s", e)
        raise
    finally:
        close_connection(cur, conn)

# -----------------------------------------------------------------------------
# Orchestrator
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
) -> None:
    """
    High-level driver:
      1) Determine dates to process via audit table
      2) For each date: restart if FAILED/RUNNING (stale), start new if NOT_STARTED, skip if COMPLETED
      3) Stream in deterministic order and update audit per chunk
    """
    table_name_uc = table_name.upper()
    logger.info("Begin processing Oracle->MinIO for %s up to %s", table_name_uc, current_business_loaddt)

    dates = get_load_status_and_dates(config_audit, table_name_uc, current_business_loaddt)
    if not dates:
        logger.info("Nothing to process.")
        return

    for date_info in dates:
        biz_dt = date_info["business_loaddt"]
        status = date_info["status"]
        restart_point = int(date_info.get("restart_point") or 0)
        logger.info("Processing %s status=%s restart_point=%s", biz_dt, status, restart_point)

        try:
            if status == "RUNNING":
                # Treat as stale and restart
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
                )
            elif status == "COMPLETED":
                logger.info("Date %s already COMPLETED. Skipping.", biz_dt)
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to process %s: %s", biz_dt, e)
            # continue to next date
            continue

    logger.info("All required dates processed for %s", table_name_uc)

# -----------------------------------------------------------------------------
# Example usage (read from env instead of hard-coding secrets)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    oracle_connection_config = {
        "user": os.environ.get("ORACLE_USER", "uds"),
        "password": os.environ.get("ORACLE_PASSWORD"),
        "dsn": os.environ.get("ORACLE_DSN"),  # e.g. "ora-scn-pr.dwhmartr:21521/martechdwhprd"
    }
    if not oracle_connection_config["password"] or not oracle_connection_config["dsn"]:
        raise SystemExit("Please set ORACLE_PASSWORD and ORACLE_DSN environment variables")

    minio_connection_config = {
        "endpoint": os.environ.get("MINIO_ENDPOINT"),
        "access_key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret_key": os.environ.get("MINIO_SECRET_KEY"),
        "bucket_name": os.environ.get("MINIO_BUCKET", "sbi-test"),
        "secure": os.environ.get("MINIO_SECURE", "true").lower() == "true",
    }
    if not (minio_connection_config["endpoint"] and minio_connection_config["access_key"] and minio_connection_config["secret_key"]):
        raise SystemExit("Please set MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY environment variables")

    audit_config = {
        "target": oracle_connection_config,  # reuse same DB creds for audit
        "schema": os.environ.get("AUDIT_SCHEMA", "UDS"),
        "audit_table": os.environ.get("AUDIT_TABLE", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
        "output_file_path": os.environ.get("AUDIT_OUTPUT_BASE", "segmentation/segmentdb_6550/input_data/customer_data/customer_profile"),
    }

    table_to_extract = os.environ.get("ORACLE_TABLE", "CDP_UNICA_REFINED.CUSTOMER_PROFILE")
    minio_output_path = os.environ.get("MINIO_OUTPUT_BASE", "segmentation/segmentdb_6550/input_data/customer_data/customer_profile")
    current_business_load_date = os.environ.get("BUSINESS_LOAD_DATE", datetime.now(IST).strftime("%Y-%m-%d"))

    # If your table has a good stable key, set ORDER_BY=
    order_by_col = os.environ.get("ORDER_BY")  # e.g. "CUSTOMER_ID" or "(BUSINESS_LOADDT, ID)"

    logger.info("Starting process_oracle_to_minio_with_dependencies")
    process_oracle_to_minio_with_dependencies(
        oracle_config=oracle_connection_config,
        minio_config=minio_connection_config,
        config_audit=audit_config,
        table_name=table_to_extract,
        base_object_path=minio_output_path,
        current_business_loaddt=current_business_load_date,
        chunk_size=int(os.environ.get("CHUNK_SIZE", "50000")),
        compression=os.environ.get("PARQUET_COMPRESSION", "snappy"),
        order_by=order_by_col,
    )
