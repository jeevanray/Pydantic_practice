import os
import io
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover - missing dependency
    MongoClient = None

from minio_handler import init_minio_client, upload_table, delete_file
from audit import prepare_auditing, update_audit_record_strict, initialize_restart_audit_log
from utilities import load_config_from_yaml
from constants import DATETIMEFORMAT
from logconfig import get_logger


# Use shared logger
logger = get_logger("Mongo_MinIO")


IST = pytz.timezone("Asia/Kolkata")


def generate_object_path(base_path: str, business_loaddt: str, load_type: str = "delta",
                         delta_column_value: Optional[str] = None,
                         sub_folder: Optional[str] = None) -> str:
    """Lightweight path generation compatible with existing driver."""
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


def create_arrow_table_from_dicts(docs: List[Dict[str, Any]], fields: List[str]) -> pa.Table:
    """Create a PyArrow table from a list of dicts using the provided field order.

    If docs is empty, create an empty table with string columns.
    """
    if not docs:
        arrays = [pa.array([], type=pa.string()) for _ in fields]
        return pa.Table.from_arrays(arrays, names=fields)

    arrays = []
    for f in fields:
        col_vals = [d.get(f) for d in docs]
        # Let pyarrow infer type; convert naive datetimes to aware IST
        processed = []
        for v in col_vals:
            if isinstance(v, datetime) and v.tzinfo is None:
                processed.append(IST.localize(v))
            else:
                processed.append(v)
        try:
            arrays.append(pa.array(processed))
        except Exception:
            # Fallback to strings when Arrow can't infer
            arrays.append(pa.array([None if v is None else str(v) for v in processed], type=pa.string()))

    return pa.Table.from_arrays(arrays, names=fields)


def upload_arrow_table_parquet(minio_client, table: pa.Table, bucket_name: str,
                               object_path: str, compression: str = "snappy") -> None:
    """Upload arrow table using shared upload_table helper (multipart-capable)."""
    # Delegate to minio_handler.upload_table which handles parquet serialization and multipart
    return upload_table(minio_client, table, bucket_name, object_path, compression=compression)


def mongo_to_minio_parquet(
    mongo_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    db_name: str,
    collection_name: str,
    base_object_path: str,
    business_loaddt: str,
    *,
    projection_fields: Optional[List[str]] = None,
    query: Optional[Dict[str, Any]] = None,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    load_type: str = 'delta',
    delta_column_value: Optional[str] = None,
) -> None:
    """Read from MongoDB, project fields, and write parquet files to MinIO.

    This function mimics the oracle driver audit/update behavior but for MongoDB.
    """
    if MongoClient is None:
        raise RuntimeError("pymongo is required for MongoDB access. Install pymongo in your environment.")

    if not projection_fields:
        raise ValueError("projection_fields must be provided as a list of field names to extract")

    try:
        datetime.strptime(business_loaddt, "%Y-%m-%d")
    except ValueError:
        raise ValueError("business_loaddt must be in YYYY-MM-DD format")

    extraction_start = datetime.now(IST)

    effective_object_path = generate_object_path(base_object_path, business_loaddt, load_type, delta_column_value)

    audit_log = prepare_auditing()
    audit_log.update({
        "source_table": f"{db_name}.{collection_name}",
        "business_loaddt": business_loaddt,
        "delta_column_value": delta_column_value,
        "load_type": load_type,
        "task_startts": extraction_start.strftime(DATETIMEFORMAT),
        "status": "RUNNING",
        "minio_filepath": effective_object_path,
    })

    initialize_restart_audit_log(config_audit, audit_log, business_loaddt, delta_column_value)

    # Connect to Mongo
    client = MongoClient(**mongo_config)
    db = client[db_name]
    coll = db[collection_name]

    cursor = coll.find(filter=query or {}, projection={f: 1 for f in projection_fields}, batch_size=chunk_size)

    mclient = init_minio_client(minio_config)

    chunk_index = int(audit_log.get("restart_point", 0) or 0)
    total_records = int(audit_log.get("total_records", 0) or 0)

    docs_buffer: List[Dict[str, Any]] = []
    try:
        for doc in cursor:
            docs_buffer.append(doc)
            if len(docs_buffer) >= chunk_size:
                table_chunk = create_arrow_table_from_dicts(docs_buffer, projection_fields)
                clean_delta = str(delta_column_value).replace("-", "") if delta_column_value else ""
                object_name = (
                    f"{effective_object_path}/{collection_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
                    if load_type == 'historic'
                    else f"{effective_object_path}/{collection_name.replace('.', '_')}_{chunk_index:06d}.parquet"
                ).replace("//", "/")

                # Use minio_handler.upload_table which handles parquet serialization and multipart upload
                upload_table(mclient, table_chunk, minio_config["bucket_name"], object_name, compression=compression)

                recs = table_chunk.num_rows
                total_records += recs

                now_ist = datetime.now(IST)
                current_run_time = (now_ist - extraction_start).total_seconds()

                audit_log.update({
                    "total_records": total_records,
                    "status": "RUNNING",
                    "task_endts": now_ist.strftime(DATETIMEFORMAT),
                    "task_exec_secs": current_run_time,
                    "extraction_time": current_run_time,
                    "restart_point": chunk_index + 1,
                    "minio_filepath": effective_object_path,
                    "mongodb_record_cnt": total_records,
                })

                update_audit_record_strict(config_audit, audit_log)

                logger.info("Uploaded chunk %s (%s rows) to %s", chunk_index, recs, object_name)
                chunk_index += 1
                docs_buffer = []

        # flush remaining
        if docs_buffer:
            table_chunk = create_arrow_table_from_dicts(docs_buffer, projection_fields)
            object_name = f"{effective_object_path}/{collection_name.replace('.', '_')}_{chunk_index:06d}.parquet".replace("//", "/")
            upload_table(mclient, table_chunk, minio_config["bucket_name"], object_name, compression=compression)
            recs = table_chunk.num_rows
            total_records += recs

            now_ist = datetime.now(IST)
            current_run_time = (now_ist - extraction_start).total_seconds()

            audit_log.update({
                "total_records": total_records,
                "status": "RUNNING",
                "task_endts": now_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": current_run_time,
                "extraction_time": current_run_time,
                "restart_point": chunk_index + 1,
                "minio_filepath": effective_object_path,
                "mongodb_record_cnt": total_records,
            })

            update_audit_record_strict(config_audit, audit_log)
            logger.info("Uploaded final chunk %s (%s rows) to %s", chunk_index, recs, object_name)

        # final update
        final_ist = datetime.now(IST)
        total_exec = (final_ist - extraction_start).total_seconds()
        audit_log.update({
            "status": "COMPLETED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": total_exec,
            "extraction_time": total_exec,
            "mongodb_record_cnt": total_records,
        })
        update_audit_record_strict(config_audit, audit_log)

    except Exception as e:
        final_ist = datetime.now(IST)
        audit_log.update({
            "status": "FAILED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": (final_ist - extraction_start).total_seconds(),
            "mongodb_error": str(e),
        })
        try:
            update_audit_record_strict(config_audit, audit_log)
        except Exception as audit_err:
            logger.error("Audit update after failure also failed: %s", audit_err)
        logger.error("Mongo -> MinIO transfer failed: %s", e)
        raise
    finally:
        try:
            client.close()
        except Exception:
            pass
        # MinIO client has no explicit close; nothing to do


if __name__ == "__main__":
    # Ensure shared logging configured
    get_logger("Mongo_MinIO")

    try:
        mongo_cfg = {'host': os.getenv('MONGO_URI','mongodb://root:pass123@localhost:27017')}

        minio_cfg = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            'secure': os.getenv('MINIO_SECURE', 'False') in ('True', 'true', '1'),
            'bucket_name': os.getenv('MINIO_BUCKET', 'sbi-test')
        }

        audit_config = {
            'target': {
                'host': os.getenv('ORACLE_HOST', 'localhost'),
                'port': int(os.getenv('ORACLE_PORT', '1521')),
                'user': os.getenv('ORACLE_USER', 'appuser'),
                'password': os.getenv('ORACLE_PASSWORD', 'appuserpwd'),
                'service_name': os.getenv('ORACLE_SERVICE', 'XEPDB1'),
                'dsn': f"{os.getenv('ORACLE_HOST', 'localhost')}/{os.getenv('ORACLE_SERVICE', 'XEPDB1')}"
            },
            'schema': os.getenv('AUDIT_SCHEMA', 'appuser'),
            'audit_table': os.getenv('AUDIT_TABLE', 'AIRFLOW_CDP_DIAPI_RUN_LOG')
        }

        db = os.getenv('MONGO_DB', 'testdb')
        coll = os.getenv('MONGO_COLLECTION', 'CIF')
        base_path = os.getenv('MINIO_BASE_PATH', 'mongo_exports')
        current_date = datetime.now(IST).strftime("%Y-%m-%d")

        # Specify which fields to extract (simple comma separated env var)
        fields_env = os.getenv('MONGO_FIELDS', 'CIF')
        fields = [f.strip() for f in fields_env.split(',') if f.strip()]

        logger.info("Starting Mongo->MinIO export")
        mongo_to_minio_parquet(mongo_cfg, minio_cfg, audit_config, db, coll, base_path, current_date,
                               projection_fields=fields, chunk_size=int(os.getenv('CHUNK_SIZE', '50000')))

    except Exception as e:
        logger.error("CRITICAL: Main execution failed: %s", e)
        raise
