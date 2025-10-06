import os
import logging
import yaml
from typing import Dict, Any, List, Optional, Union, Sequence, Tuple

import pandas as pd  # ONLY for reading operations
import oracledb
from oracledb import LOB
import datetime
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pytz
from minio_handler import MinioHandler
from constants import DATETIMEFORMAT

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

IST = pytz.timezone('Asia/Kolkata')

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

def load_config_from_yaml(yaml_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)
    


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
    
def close_connection(cursor: Any, connection: Any) -> None:
    """Safely close cursor and connection."""
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    except Exception as e:
        logger.warning(f"Error closing database connections: {e}")

def _parse_datetime(value: Optional[str], format: str, is_date: bool = False):
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
