import os
import io
import logging
import time
import copy
from typing import Dict, Any, List, Optional, Union, Sequence, Tuple
from decimal import Decimal
from datetime import datetime, date

import pandas as pd  # ONLY for reading operations
import oracledb
from oracledb import LOB
import pytz
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pyarrow.parquet as pq

# External dependencies
from minio_handler import MinioHandler
# from cdp_diapi_adapter import get_system_config, close_connection
from constants import DATETIMEFORMAT, ORACLE_DATE

# Logging
logger = logging.getLogger("Minio_framework")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - PID:%(process)d - TID:%(thread)d - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# TZ
IST = pytz.timezone("Asia/Kolkata")

def get_oracle_connection(oracle_config: Dict[str, Any]):
    """Create an Oracle database connection."""
    try:
        connection = oracledb.connect(
            user=oracle_config["username"],
            password=oracle_config["password"],
            dsn=oracle_config["dsn"]
        )
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
        raise

def close_connection(cursor: Any, connection: Any) -> None:
    """Safely close cursor and connection."""
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    except Exception as e:
        logger.warning(f"Error closing database connections: {e}")

# UPDATED: Returns tuple (schema, type_map)
def get_oracle_table_schema(oracle_config: Dict[str, Any], table_name: str) -> Tuple[pa.Schema, Dict[str, pa.DataType]]:
    """
    Fetch table schema directly from Oracle system tables and map to PyArrow schema.
    Returns (schema, type_map) where type_map is {column_name: pa.DataType}.
    """
    schema_query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            DATA_PRECISION,
            DATA_SCALE,
            NULLABLE,
            DATA_LENGTH,
            CHAR_LENGTH
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = UPPER(:table_name)
        AND OWNER = UPPER(:schema_name)
        ORDER BY COLUMN_ID
    """

    if '.' in table_name:
        schema_name, table_only = table_name.split('.', 1)
    else:
        schema_name = oracle_config.get('schema', 'PUBLIC')
        table_only = table_name

    fields = []

    with connect_to_oracle(oracle_config) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_query, {
                'table_name': table_only.upper(),
                'schema_name': schema_name.upper()
            })

            for row in cur.fetchall():
                col_name, data_type, precision, scale, nullable, data_length, char_length = row

                # Map Oracle data types to PyArrow types
                if data_type in ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR'):
                    arrow_type = pa.string()
                elif data_type in ('CLOB', 'NCLOB', 'LONG'):
                    arrow_type = pa.string()
                elif data_type == 'NUMBER':
                    if precision is None:
                        arrow_type = pa.float64()
                    elif scale == 0 or scale is None:
                        if precision <= 9:
                            arrow_type = pa.int32()
                        elif precision <= 18:
                            arrow_type = pa.int64()
                        else:
                            arrow_type = pa.decimal128(precision, 0)
                    else:
                        if precision <= 7 and scale <= 7:
                            arrow_type = pa.float32()
                        elif precision <= 15 and scale <= 15:
                            arrow_type = pa.float64()
                        else:
                            arrow_type = pa.decimal128(min(precision, 38), min(scale, 38))
                elif data_type in ('BINARY_INTEGER', 'PLS_INTEGER'):
                    arrow_type = pa.int32()
                elif data_type == 'BINARY_FLOAT':
                    arrow_type = pa.float32()
                elif data_type == 'BINARY_DOUBLE':
                    arrow_type = pa.float64()
                elif data_type == 'DATE':
                    arrow_type = pa.date32()
                elif data_type == 'TIMESTAMP':
                    arrow_type = pa.timestamp('us')
                elif data_type.startswith('TIMESTAMP'):
                    if 'WITH TIME ZONE' in data_type:
                        arrow_type = pa.timestamp('us', tz='UTC')
                    else:
                        arrow_type = pa.timestamp('us')
                elif data_type in ('RAW', 'LONG RAW'):
                    arrow_type = pa.binary()
                elif data_type == 'BLOB':
                    arrow_type = pa.binary()
                elif data_type == 'BOOLEAN':
                    arrow_type = pa.bool_()
                elif data_type in ('JSON', 'XMLTYPE'):
                    arrow_type = pa.string()
                else:
                    arrow_type = pa.string()
                    logger.warning(f"Unknown Oracle data type '{data_type}' for column '{col_name}', defaulting to string")

                is_nullable = (nullable == 'Y')
                fields.append(pa.field(col_name, arrow_type, nullable=is_nullable))

    schema = pa.schema(fields)
    type_map: Dict[str, pa.DataType] = {f.name: f.type for f in fields}
    logger.info(f"Extracted schema for {table_name}: {len(fields)} columns")

    return schema, type_map


# NEW: Columnar sanitization - only for LOBs and essential conversions
def sanitize_column_data(column_values: List[Any], field: pa.Field) -> List[Any]:
    """
    Sanitize a single column's values. Only handle special cases like LOBs.
    Everything else passes through for Arrow casting.
    """
    sanitized_values = []
    field_type = field.type

    for value in column_values:
        if value is None:
            sanitized_values.append(None)
        elif isinstance(value, LOB):
            # Handle LOB types - main reason for sanitization
            try:
                lob_data = value.read()
                if pa.types.is_string(field_type):
                    # CLOB -> string
                    if isinstance(lob_data, bytes):
                        sanitized_values.append(lob_data.decode('utf-8', errors='ignore'))
                    else:
                        sanitized_values.append(str(lob_data) if lob_data is not None else None)
                elif pa.types.is_binary(field_type):
                    # BLOB -> binary
                    if isinstance(lob_data, bytes):
                        sanitized_values.append(lob_data)
                    else:
                        sanitized_values.append(str(lob_data).encode('utf-8') if lob_data is not None else None)
                else:
                    sanitized_values.append(lob_data)
            except Exception as e:
                logger.warning(f"Failed to read LOB for column {field.name}: {e}")
                sanitized_values.append(None)
        elif pa.types.is_date32(field_type) and isinstance(value, datetime):
            # Convert datetime to date for DATE columns
            sanitized_values.append(value.date())
        else:
            # Pass through all other values - let Arrow handle it
            sanitized_values.append(value)

    return sanitized_values


# NEW: Column casting with proper null column handling
def cast_table_columns_to_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """
    Cast each column using YOUR EXACT requested approach:
    col_idx = table.schema.get_field_index("<col>")
    table_casted = table.set_column(col_idx, "<col>", table["<col>"].cast(pa.string()))

    Special handling for completely empty/null columns.
    """
    casted_table = table

    for field in target_schema:
        if field.name not in table.column_names:
            continue

        # YOUR EXACT APPROACH: get field index
        col_idx = casted_table.schema.get_field_index(field.name)
        if col_idx == -1:
            continue

        current_column = casted_table[field.name]  # ChunkedArray

        # Only cast if types don't match
        if not current_column.type.equals(field.type):
            try:
                # SPECIAL HANDLING FOR COMPLETELY NULL COLUMNS
                if current_column.null_count == len(current_column):
                    # Column is completely empty - create array with correct Oracle dtype
                    logger.debug(f"Column {field.name} is completely null, casting to Oracle type {field.type}")

                    # Create typed null array matching Oracle schema
                    null_values = [None] * len(current_column)
                    typed_null_array = pa.array(null_values, type=field.type)
                    casted_column = pa.chunked_array([typed_null_array])

                else:
                    # NORMAL CASTING for columns with data
                    try:
                        # Try safe cast first
                        casted_column = current_column.cast(field.type, safe=True)
                        logger.debug(f"Safe cast successful for column {field.name}: {current_column.type} -> {field.type}")
                    except pa.ArrowInvalid:
                        # Fall back to unsafe cast for type coercion
                        logger.debug(f"Safe cast failed, trying unsafe cast for column {field.name}")
                        casted_column = current_column.cast(field.type, safe=False)

                # YOUR EXACT APPROACH: set_column
                casted_table = casted_table.set_column(col_idx, field.name, casted_column)
                logger.debug(f"Successfully cast column {field.name} to {field.type}")

            except Exception as e:
                logger.warning(f"Failed to cast column {field.name} from {current_column.type} to {field.type}: {e}")
                # Keep original column if casting fails
                continue

    return casted_table


# NEW: Create Arrow table from Oracle rows
def create_arrow_table_from_rows(rows: List[tuple], column_names: List[str], target_schema: pa.Schema) -> pa.Table:
    """
    Create PyArrow table directly from Oracle cursor rows.
    Apply columnar sanitization only where needed (LOBs, etc).
    """
    if not rows:
        # Return empty table with correct schema
        empty_arrays = [pa.array([], type=f.type) for f in target_schema]
        return pa.Table.from_arrays(empty_arrays, names=column_names)

    # Transpose rows to columns for columnar processing
    columns_data = list(zip(*rows))

    # Build arrays column by column with minimal sanitization
    arrays = []
    for i, (column_data, field) in enumerate(zip(columns_data, target_schema)):
        # Sanitize only what's needed (LOBs, dates)
        sanitized_column = sanitize_column_data(list(column_data), field)
        # Create array without explicit type - let Arrow infer, then cast later
        arrays.append(pa.array(sanitized_column))

    # Create table and cast to target schema
    table = pa.Table.from_arrays(arrays, names=column_names)
    return cast_table_columns_to_schema(table, target_schema)


# NEW: Pure PyArrow upload - NO pandas
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def upload_arrow_table_parquet(minio_client: MinioHandler, table: pa.Table,
                               object_path: str, compression: str = "snappy") -> None:
    """
    Upload PyArrow Table directly as parquet. NO pandas involved.
    """
    try:
        # Try MinIO handler's table upload if available
        if hasattr(minio_client, "upload_table"):
            return minio_client.upload_table(
                table=table,
                object_path=object_path,
                format="parquet",
                compression=compression
            )

        # Fallback: write to buffer and upload
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=compression)
        data = buffer.getvalue()

        if hasattr(minio_client, "put_object"):
            minio_client.put_object(object_path, data, len(data),
                                  content_type="application/octet-stream")
        else:
            raise RuntimeError("MinioHandler must provide upload_table or put_object method")

    except Exception as e:
        logger.error(f"Failed to upload Arrow table to {object_path}: {e}")
        raise


# Keep existing Oracle connection function
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def connect_to_oracle(oracle_conf: Dict[str, Any]) -> oracledb.Connection:
    """Connect to Oracle database with retry logic."""
    logger.info("Connecting to Oracle DB")
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
        raise RuntimeError(f"Oracle connection failed: {e}")


# Keep existing audit functions
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


def load_config_from_yaml(yaml_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


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


# Keep existing audit functions (using pandas for READING only)
def _parse_datetime(value: Optional[str], format: str, is_date: bool = False) -> Optional[Union[datetime, datetime.date]]:
    """Helper to parse datetime or date strings."""
    if isinstance(value, str) and value:
        parsed = datetime.strptime(value, format)
        return parsed.date() if is_date else IST.localize(parsed)
    return None


def _ensure_time_fields(audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize types for time fields and booleans prior to MERGE."""
    audit_data_new = audit_data.copy()
    datetime_fields = ["task_startts", "task_endts"]
    date_fields = ["business_loaddt"]

    audit_data_new["cdp_db_count_validation"] = 'Y' if audit_data_new.get("cdp_db_count_validation") else 'N'

    for field in datetime_fields:
        audit_data_new[field] = _parse_datetime(audit_data_new.get(field), DATETIMEFORMAT)

    for field in date_fields:
        audit_data_new[field] = _parse_datetime(audit_data_new.get(field), "%Y-%m-%d", is_date=True)

    now_ist = datetime.now(IST)
    audit_data_new["updated_at_ts"] = now_ist
    if not audit_data_new.get("created_at_ts"):
        audit_data_new["created_at_ts"] = now_ist
    return audit_data_new


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def initialize_restart_audit_log(config_audit: Dict[str, Any], audit_log: Dict[str, Any], aud_dt: str,
                                delta_column_value: Optional[str] = None) -> None:
    """Load existing audit record for restart."""
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
        finally:
            close_connection(cur, conn)

    # All attempts failed
    error_msg = f"CRITICAL: Audit update failed after {max_attempts} attempts"
    logger.error(error_msg)
    if last_error:
        raise RuntimeError(f"{error_msg}: {last_error}")
    else:
        raise RuntimeError(error_msg)


# Keep existing functions that use pandas for READING only
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def get_historic_load_status(config_audit: Dict[str, Any], source_table: str,
                           current_business_loaddt: str, delta_column: str,
                           delta_column_type: str, delta_column_format:str,
                           oracle_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get historic load status - uses pandas for READING (allowed)."""
    file_name = f'source_delta_{source_table.replace(".", "_")}.parquet'
    parquet_basepath = "/opt/airflow/etl_inward_files/CDP_minio/"
    parquet_file = os.path.join(parquet_basepath, file_name)
    os.makedirs(parquet_basepath, exist_ok=True)

    try:
        # Load distinct delta values from Parquet or refresh from source
        with connect_to_oracle(oracle_config) as source_conn:
            if not os.path.exists(parquet_file):
                logger.info("Parquet file %s not found. Refreshing all distinct delta values.", parquet_file)
                query = f"SELECT /*+ PARALLEL(4) */ DISTINCT {delta_column} as delta_value FROM {source_table} WHERE {delta_column} IS NOT NULL ORDER BY {delta_column}"
                source_deltas_df = pd.read_sql(query, source_conn)
                if delta_column_type:
                    if delta_column_type.lower() == 'timestamp':
                        source_deltas_df['DELTA_VALUE'] = (pd.to_datetime(source_deltas_df['DELTA_VALUE'],
                                                                          format=delta_column_format).dt.strftime('%Y-%m-%d'))
                    elif delta_column_type.lower() == 'date' and delta_column_format is not None:
                        source_deltas_df['DELTA_VALUE'] = (pd.to_datetime(source_deltas_df['DELTA_VALUE'],
                                                            format=delta_column_format).dt.strftime('%Y-%m-%d')).drop_duplicates()
                source_deltas_df.to_parquet(parquet_file, index=False)
                logger.info("Saved refreshed distinct delta values to %s.", parquet_file)
            else:
                logger.debug("Using cached parquet file %s", parquet_file)
                source_deltas_df = pd.read_parquet(parquet_file)  # PANDAS READING - ALLOWED

        all_deltas = source_deltas_df['DELTA_VALUE'].astype(str).tolist()
        last_delta = all_deltas[-1]
        if not all_deltas:
            logger.info("No delta values found for %s in Parquet file.", source_table)
            return []

        # Get audit record for completed deltas and status
        with connect_to_oracle(config_audit["target"]) as audit_conn:
            processed_deltas_query = f"""
                SELECT
                    business_loaddt,
                    NVL(status, 'NOT_STARTED') AS status,
                    NVL(restart_point, 0) AS restart_point,
                    NVL(total_records, 0) AS total_records,
                    delta_column_value,
                    NVL(load_type, 'historic') AS load_type,
                    NVL(task_exec_secs, 0) AS task_exec_secs,
                    NVL(extraction_time, 0) AS extraction_time,
                    aerospike_error
                FROM {config_audit["schema"]}.{config_audit["audit_table"]}
                WHERE source_table = :src
                AND load_type = 'historic'
                ORDER BY updated_at_ts DESC NULLS LAST
                FETCH FIRST 1 ROW ONLY
            """
            cur = audit_conn.cursor()
            cur.execute(processed_deltas_query, {"src": source_table})
            row = cur.fetchone()

            base_status = "NOT_STARTED"
            base_restart_point = 0
            base_total_records = 0
            base_exec_secs = 0
            base_extraction_time = 0
            base_delta_value = None
            biz_str = current_business_loaddt

            if row:
                business_dt, status, restart_point, total_records, delta_val, load_type, task_exec_secs, extraction_time, aerospike_error = row
                if isinstance(aerospike_error, oracledb.LOB):
                    aerospike_error = aerospike_error.read()
                base_status = status
                base_restart_point = int(restart_point or 0)
                base_total_records = int(total_records or 0)
                base_exec_secs = float(task_exec_secs or 0)
                base_extraction_time = float(extraction_time or 0)
                base_delta_value = delta_val
                biz_str = (business_dt.strftime("%Y-%m-%d") if hasattr(business_dt, "strftime") else str(business_dt)) if business_dt else current_business_loaddt

            # Find unprocessed deltas
            try:
                processed_index = all_deltas.index(base_delta_value) if base_delta_value else -1
            except ValueError:
                processed_index = -1
            unprocessed_deltas = all_deltas[processed_index+1:]

            if last_delta not in unprocessed_deltas:
                try:
                    with connect_to_oracle(oracle_config) as source_conn:
                            logger.info("Parquet file %s outdated. Refreshing all distinct delta values.", parquet_file)
                            query = f"SELECT /*+ PARALLEL(4) */ DISTINCT {delta_column} as delta_value FROM {source_table} WHERE {delta_column} IS NOT NULL ORDER BY {delta_column}"
                            source_deltas_df = pd.read_sql(query, source_conn)
                            if delta_column_type:
                                if delta_column_type.lower() == 'timestamp':
                                    source_deltas_df['DELTA_VALUE'] = (pd.to_datetime(source_deltas_df['DELTA_VALUE'],
                                                                                    format=delta_column_format).dt.strftime('%Y-%m-%d')).drop_duplicates()
                            source_deltas_df.to_parquet(parquet_file, index=False)
                            logger.info("Saved refreshed distinct delta values to %s.", parquet_file)

                except Exception  as e:
                    logger.error("Could not refresh the parquet file for delta_value")
                    raise e

            if not unprocessed_deltas:
                logger.info("All historic deltas completed for %s", source_table)
                return []

            logger.info("Found %s unprocessed deltas for processing", len(unprocessed_deltas))

            # Create job entries
            processed_jobs: List[Dict[str, Any]] = []
            for delta in unprocessed_deltas:
                if delta == base_delta_value and base_status != 'COMPLETED':
                    # Current delta still in progress
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
                    # New delta
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

            return processed_jobs

    except Exception as e:
        logger.error("CRITICAL: Failed to get historic load status: %s", e, exc_info=True)
        raise RuntimeError(f"Historic load status query failed: {e}")


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
                cur.execute(query, {"current_dt": current_business_loaddt, "fmt": ORACLE_DATE, "src": source_table})
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
                    cur.execute(completed_check_query, {"current_dt": current_business_loaddt, "fmt": ORACLE_DATE, "src": source_table})
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def get_load_status_and_dates(config_audit: Dict[str, Any], source_table: str,
                             current_business_loaddt: str, load_type: str = 'delta',
                             delta_column: Optional[str] = None,
                             delta_column_type: Optional[Dict[str, Any]] = None,
                             delta_column_format: Optional[Dict[str, Any]] = None,
                             oracle_config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Returns list of incomplete jobs only - skips COMPLETED jobs."""
    if load_type == 'historic': # and delta_column and oracle_config:
        logger.info(f"Inside Historic get_historic_load_status method, oracle_config {oracle_config}") 
        return get_historic_load_status(config_audit, source_table, current_business_loaddt, delta_column,delta_column_type,delta_column_format, oracle_config)
    else:
        return get_delta_load_status(config_audit, source_table, current_business_loaddt)


# MAIN FUNCTION: Rewritten with pure PyArrow
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
    delta_column_type: Optional[str] = None
) -> None:
    """
    Extract data from Oracle to MinIO parquet using pure PyArrow.
    NO PANDAS in the write path - only PyArrow for optimal storage.
    """
    # Validation
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

    # Initialize audit logging
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

    # Initialize restart audit log
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

    if delta_column_type == 'timestamp':
        where_part = (f" WHERE {delta_column} >= DATE '{delta_column_value}' AND {delta_column} < DATE '{delta_column_value}'  + INTERVAL '1' DAY" if load_type == 'historic' else "")
        # where_part = (f" WHERE {delta_column} = TO_TIMESTAMP('{delta_column_value}', 'YYYY-MM-DD HH24:MI:SS')" if load_type == 'historic' and delta_column and delta_column_value else "")
    # elif delta_column_type == 'date':
    else:
        where_part = f" WHERE {delta_column} = TO_DATE('{delta_column_value}', 'YYYY-MM-DD')" if load_type == 'historic' and delta_column and delta_column_value else ""
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT * FROM {table_name}{where_part}{order_clause}" #fetch first 2000000 rows only"
    logger.info(f"where_part:{where_part},delta_column: {delta_column}, delta_column_value {delta_column_value} ")

    conn = None
    cur = None

    try:
        conn = connect_to_oracle(oracle_config)
        mclient = MinioHandler(minio_config)
        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))
        logger.info("Executing SELECT for streaming: %s", select_sql)
        cur.execute(select_sql)

        # Skip to restart point if needed
        for _ in range(start_chunk_index):
            skipped = cur.fetchmany(chunk_size)
            if not skipped:
                break

        chunk_index = start_chunk_index
        column_names = [field.name for field in oracle_schema]

        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                logger.info("No more data to process for %s", table_name)
                break

            # PURE PYARROW PROCESSING - NO PANDAS
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
                # PURE PYARROW UPLOAD - NO PANDAS
                upload_arrow_table_parquet(mclient, table_chunk, object_name, compression=compression)
                recs = table_chunk.num_rows
                if chunk_index % 10 == 0:
                    logger.info("Successfully uploaded chunk %s (%s rows) with Arrow-optimized schema",
                              chunk_index, recs)
            except Exception as e:
                logger.error("Failed to upload chunk %s to MinIO: %s", chunk_index, e)
                raise RuntimeError(f"MinIO upload failed for chunk {chunk_index}: {e}")

            # Update audit log
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
            logger.info(f'audit_log:{audit_log}')

            # Update audit with error handling
            try:
                update_audit_record_strict(config_audit, audit_log)
                logger.debug("Chunk %s audit update completed", chunk_index)
            except Exception as e:
                logger.error("Audit update failed for chunk %s; deleting uploaded object: %s/%s",
                           chunk_index, bucket, object_name)
                try:
                    if hasattr(mclient, "delete_file"):
                        mclient.delete_file(object_name, bucket)
                    elif hasattr(mclient, "remove_object"):
                        mclient.remove_object(bucket, object_name)
                    logger.info("Cleaned up uploaded object after audit failure")
                except Exception as del_err:
                    logger.error("Failed to delete object after audit failure: %s", del_err)
                raise RuntimeError(f"Chunk {chunk_index} audit update failed: {e}")

            total_records = cumulative_total
            if chunk_index % 10 == 0:
                logger.info("Chunk %s completed (%s rows) -> %s/%s", chunk_index, recs, bucket, object_name)
            chunk_index += 1

        # Final audit update
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

        if hasattr(mclient, 'close'):
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
    delta_column_type: Optional[str] = None,
    delta_column_format: Optional[str] = None
) -> None:
    """Sequential processing - processes ALL incomplete jobs in order."""
    logger.info(f"oracle_config: {oracle_config}")
    table_name_uc = table_name.upper()
    logger.info("Begin processing Oracle->MinIO for %s up to %s (load_type=%s)",
               table_name_uc, current_business_loaddt, load_type)
    

    # Get jobs to process
    dates = get_load_status_and_dates(
        config_audit, table_name_uc, current_business_loaddt,
        load_type, delta_column,delta_column_type,delta_column_format, oracle_config=oracle_config
    )

    if not dates:
        logger.info("No jobs to process - all completed or no jobs found.")
        return

    # Process each job sequentially
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
                delta_column_type=delta_column_type
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
                delta_column_format=delta_column_format
            )
        except Exception as e:
            logger.error("CRITICAL: Failed to process %s: %s", table_name, e)
            raise RuntimeError(f"Object processing failed for {table_name}: {e}")


# Main execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    try:
        # conn_config = get_system_config()
        conn_config = {
            "target": {
                "host": os.getenv("ORACLE_HOST", "localhost"),
                "port": int(os.getenv("ORACLE_PORT", "1521")),
                "user": os.getenv("ORACLE_USER", "appuser"),
                "password": os.getenv("ORACLE_PASSWORD", "appuserpwd"),  # Consider using a secret manager
                "service_name": os.getenv("ORACLE_SERVICE", "XEPDB1"),
                "dsn": f"{os.getenv('ORACLE_HOST', 'localhost')}/{os.getenv('ORACLE_SERVICE', 'XEPDB1')}"
            },
            "minio": {
                "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
                "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),  # Consider using a secret manager
                "secure": os.getenv("MINIO_SECURE", False),
                "bucket_name": os.getenv("MINIO_BUCKET", "sbi-test")
            }
        }
        config_yaml = "db2_to_uds_config.yml" #"etl_configs/db2_to_uds_config.yml"
        config_data = load_config_from_yaml(config_yaml)["uds_to_minio"]
        config_data["schema"] = "appuser"
        current_date = datetime.now(IST).strftime("%Y-%m-%d")

        logger.info("Starting configuration-based processing with pure PyArrow optimization")
        logger.info(f"config_data:{config_data}")
        process_from_config(config_data, conn_config, current_date)
        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error("CRITICAL: Main execution failed: %s", e)
        raise RuntimeError(f"Application failed: {e}")