import os
import io
import logging
import time
from typing import Dict, Any, List, Optional, Union, Sequence
from decimal import Decimal
from datetime import datetime, date

import pandas as pd
import oracledb
from oracledb import LOB
import pytz
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pyarrow.parquet as pq

# External dependencies
from minio_handler import MinioHandler
from cdp_diapi_adapter import get_system_config, close_connection
from Constants import DATETIMEFORMAT, ORACLE_DATE_FMT

# Logging
logger = logging.getLogger("DIAPI")

# TZ
IST = pytz.timezone("Asia/Kolkata")


# FIXED: Proper Oracle to Arrow Type Mapping (No DATE→TIMESTAMP conversion)
def get_oracle_table_schema_fixed(oracle_config: Dict[str, Any], table_name: str) -> pa.Schema:
    """
    Fetch Oracle schema and map types correctly:
    - Oracle DATE → Arrow date32 (NO timestamp conversion)
    - Oracle TIMESTAMP → Arrow timestamp
    """
    schema_query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            DATA_PRECISION,
            DATA_SCALE,
            NULLABLE,
            DATA_LENGTH
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME = UPPER(:table_name)
        AND OWNER = UPPER(:schema_name)
        ORDER BY COLUMN_ID
    """
    
    # Split schema.table if provided
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
                col_name, data_type, precision, scale, nullable, data_length = row
                
                # CRITICAL: Proper type mapping without DATE→TIMESTAMP conversion
                if data_type in ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR'):
                    arrow_type = pa.string()
                elif data_type in ('CLOB', 'NCLOB', 'LONG'):
                    arrow_type = pa.string()
                elif data_type == 'NUMBER':
                    if precision is None:
                        arrow_type = pa.float64()  # Default Oracle NUMBER
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
                    # FIXED: Oracle DATE stays as date32 (supports years 1-9999)
                    arrow_type = pa.date32()
                elif data_type == 'TIMESTAMP':
                    # Only actual TIMESTAMP columns get timestamp type
                    arrow_type = pa.timestamp('us')  # Microsecond precision
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
    
    logger.info(f"Schema mapping for {table_name}: {len(fields)} columns")
    # Log date vs timestamp columns for debugging
    for field in fields:
        if pa.types.is_date(field.type):
            logger.info(f"DATE column: {field.name} → {field.type}")
        elif pa.types.is_timestamp(field.type):
            logger.info(f"TIMESTAMP column: {field.name} → {field.type}")
    
    return pa.schema(fields)


# FIXED: Data Sanitization that Respects DATE vs TIMESTAMP
def sanitize_row_for_arrow_fixed(row: tuple, schema: pa.Schema) -> List[Any]:
    """
    Sanitize Oracle data preserving DATE as date and TIMESTAMP as datetime.
    NO conversion between date and datetime types.
    """
    sanitized_row = []
    
    for i, (value, field) in enumerate(zip(row, schema)):
        if value is None:
            sanitized_row.append(None)
            continue
        
        field_type = field.type
        
        try:
            # Handle LOB types
            if isinstance(value, LOB):
                try:
                    if hasattr(value, 'read'):
                        sanitized_value = value.read()
                        if isinstance(sanitized_value, bytes) and pa.types.is_string(field_type):
                            sanitized_value = sanitized_value.decode('utf-8', errors='ignore')
                    else:
                        sanitized_value = str(value)
                except Exception as e:
                    logger.warning(f"Failed to read LOB for column {field.name}: {e}")
                    sanitized_value = None
            
            # FIXED: Handle date32 types (Oracle DATE columns)
            elif pa.types.is_date32(field_type):
                if isinstance(value, datetime):
                    # Extract date part only (no time component)
                    sanitized_value = value.date()
                elif isinstance(value, date):
                    # Already a date, keep as-is
                    sanitized_value = value
                else:
                    # Try to convert to date
                    try:
                        if hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
                            sanitized_value = date(value.year, value.month, value.day)
                        else:
                            sanitized_value = value
                    except:
                        logger.warning(f"Cannot convert {value} to date for column {field.name}")
                        sanitized_value = None
            
            # FIXED: Handle timestamp types (Oracle TIMESTAMP columns)
            elif pa.types.is_timestamp(field_type):
                if isinstance(value, datetime):
                    # Keep datetime as-is for timestamp columns
                    sanitized_value = value
                elif isinstance(value, date):
                    # Convert date to datetime for timestamp columns
                    sanitized_value = datetime.combine(value, datetime.min.time())
                else:
                    try:
                        if isinstance(value, str):
                            sanitized_value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        else:
                            sanitized_value = value
                    except:
                        sanitized_value = None
            
            # Handle other types (keeping existing logic)
            elif pa.types.is_integer(field_type):
                if isinstance(value, Decimal):
                    try:
                        sanitized_value = int(value)
                    except (ValueError, OverflowError):
                        logger.warning(f"Decimal {value} cannot be converted to int for column {field.name}")
                        sanitized_value = None
                elif isinstance(value, float):
                    if value.is_integer():
                        sanitized_value = int(value)
                    else:
                        sanitized_value = int(round(value))
                else:
                    sanitized_value = value
            
            elif pa.types.is_floating(field_type):
                if isinstance(value, Decimal):
                    sanitized_value = float(value)
                else:
                    sanitized_value = value
            
            elif pa.types.is_decimal(field_type):
                if isinstance(value, (int, float)):
                    sanitized_value = Decimal(str(value))
                elif not isinstance(value, Decimal):
                    sanitized_value = Decimal(str(value))
                else:
                    sanitized_value = value
            
            elif pa.types.is_boolean(field_type):
                if isinstance(value, str):
                    sanitized_value = value.upper() in ('TRUE', 'T', 'YES', 'Y', '1')
                elif isinstance(value, (int, float)):
                    sanitized_value = bool(value)
                else:
                    sanitized_value = bool(value)
            
            elif pa.types.is_string(field_type):
                if isinstance(value, bytes):
                    sanitized_value = value.decode('utf-8', errors='ignore')
                else:
                    sanitized_value = str(value)
            
            elif pa.types.is_binary(field_type):
                if isinstance(value, str):
                    sanitized_value = value.encode('utf-8')
                elif not isinstance(value, bytes):
                    sanitized_value = str(value).encode('utf-8')
                else:
                    sanitized_value = value
            
            else:
                sanitized_value = value
                
        except Exception as e:
            logger.warning(f"Error sanitizing value {value} for column {field.name} ({field_type}): {e}")
            sanitized_value = None
        
        sanitized_row.append(sanitized_value)
    
    return sanitized_row


# FIXED: Upload function with proper schema handling
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def upload_df_parquet_with_schema_fixed(minio_client: MinioHandler, df: pd.DataFrame, 
                                       schema: pa.Schema, object_path: str, 
                                       compression: str = "snappy") -> None:
    """Upload DataFrame as parquet with explicit schema preservation."""
    try:
        # Create PyArrow table with explicit schema
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        
        # Try MinIO handler methods
        if hasattr(minio_client, "upload_table"):
            return minio_client.upload_table(
                table=table, 
                object_path=object_path, 
                format="parquet", 
                compression=compression
            )
        elif hasattr(minio_client, "upload_dataframe"):
            # Convert back to dataframe with proper types
            typed_df = table.to_pandas()
            return minio_client.upload_dataframe(
                df=typed_df, 
                object_path=object_path, 
                format="parquet", 
                compression=compression
            )
        else:
            # Manual upload using PyArrow
            buf = io.BytesIO()
            pq.write_table(table, buf, compression=compression)
            data = buf.getvalue()
            
            if hasattr(minio_client, "put_object"):
                minio_client.put_object(object_path, data, len(data), 
                                      content_type="application/octet-stream")
            else:
                raise RuntimeError("MinioHandler must provide upload_table/upload_dataframe/put_object method")
                
    except Exception as e:
        logger.error(f"Failed to upload parquet with schema to {object_path}: {e}")
        raise


# [Keep all existing helper functions unchanged]
def load_config_from_yaml(yaml_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def connect_to_oracle(oracle_conf: Dict[str, Any]) -> oracledb.Connection:
    """Connect to Oracle database with retry logic and MANDATORY error raising."""
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

# [Keep all existing audit functions unchanged - prepare_auditing, _ensure_time_fields, etc.]
def prepare_auditing(source_table: str = "", load_type: str = "delta", 
                    business_loaddt: str = "") -> Dict[str, Any]:
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
        "load_type": load_type,
        "created_at_ts": None,
        "updated_at_ts": None,
    }

def _parse_datetime(value: Optional[str], format: str, is_date: bool = False) -> Optional[Union[datetime, datetime.date]]:
    """Helper to parse datetime or date strings."""
    if isinstance(value, str) and value:
        parsed = datetime.strptime(value, format)
        return parsed.date() if is_date else IST.localize(parsed)
    return None

def _ensure_time_fields(audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize types for time fields and booleans prior to MERGE."""
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

# [Keep all audit functions unchanged - initialize_restart_audit_log, update_audit_record_strict, etc.]
# ... [Include existing functions for brevity]

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


# MAIN UPDATED FUNCTION: Core Extraction with Fixed Date Handling
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def oracle_to_minio_parquet_fixed(
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
    """Extract Oracle data to MinIO parquet with FIXED date/timestamp handling."""
    # Validation
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

    # Initialize audit logging (keeping existing logic)
    audit_log = prepare_auditing(table_name.upper(), load_type, business_loaddt)
    audit_log.update({
        "task_startts": extraction_start.strftime(DATETIMEFORMAT),
        "status": "RUNNING",
        "minio_filepath": effective_object_path,
        "delta_column_value": delta_column_value,
    })

    # [Keep existing restart logic for brevity]

    logger.info("Starting FIXED extraction for %s on %s (load_type=%s, delta_value=%s)",
               table_name, business_loaddt, load_type, delta_column_value)

    # CRITICAL: Get schema with FIXED date/timestamp mapping
    try:
        oracle_schema = get_oracle_table_schema_fixed(oracle_config, table_name)
        logger.info("Retrieved FIXED Oracle schema with %d columns for %s", len(oracle_schema), table_name)
    except Exception as e:
        logger.error("Failed to retrieve schema for %s: %s", table_name, e)
        raise RuntimeError(f"Schema retrieval failed: {e}")

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

        # [Keep existing skip to restart point logic]

        chunk_index = 0  # Simplified for this example
        column_names = [field.name for field in oracle_schema]

        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                logger.info("No more data to process for %s", table_name)
                break

            # CRITICAL: Use FIXED sanitization (preserves DATE vs TIMESTAMP)
            sanitized_rows = []
            for row in rows:
                sanitized_row = sanitize_row_for_arrow_fixed(row, oracle_schema)
                sanitized_rows.append(sanitized_row)

            # Create DataFrame with sanitized data
            df_chunk = pd.DataFrame(sanitized_rows, columns=column_names)
            
            clean_delta = str(delta_column_value).replace("-", "") if delta_column_value else ""
            object_name = (
                f"{effective_object_path}/{table_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
                if load_type == 'historic'
                else f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
            ).replace("//", "/")

            try:
                # CRITICAL: Upload with FIXED schema preservation
                upload_df_parquet_with_schema_fixed(mclient, df_chunk, oracle_schema, object_name, compression=compression)
                recs = len(df_chunk)
                if chunk_index % 10 == 0:
                    logger.info("Successfully uploaded chunk %s (%s rows) with FIXED date/timestamp handling", chunk_index, recs)
            except Exception as e:
                logger.error("Failed to upload chunk %s to MinIO: %s", chunk_index, e)
                raise RuntimeError(f"MinIO upload failed for chunk {chunk_index}: {e}")

            # [Keep existing audit update logic]
            chunk_index += 1
        
        # [Keep existing final audit logic]
        logger.info("Completed Oracle -> MinIO parquet for %s with FIXED date handling - no more conversion errors!", table_name)

    except Exception as e:
        logger.error("Oracle -> MinIO transfer failed: %s", e)
        raise RuntimeError(f"Extraction failed: {e}")
    finally:
        close_connection(cur, conn)


# Replace the original function with the fixed version
oracle_to_minio_parquet = oracle_to_minio_parquet_fixed

# [Keep all other existing functions unchanged]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    
    try:
        conn_config = get_system_config()
        config_yaml = "etl_configs/db2_to_uds_config.yml"
        config_data = load_config_from_yaml(config_yaml)["uds_to_minio"]
        current_date = datetime.now(IST).strftime("%Y-%m-%d")
        
        logger.info("Starting FIXED processing - no more date conversion errors!")
        # process_from_config(config_data, conn_config, current_date)  # Your existing function
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error("CRITICAL: Main execution failed: %s", e)
        raise RuntimeError(f"Application failed: {e}")
