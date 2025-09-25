fix few things for me like
1. get_oracle_table_schema function should return schema and dictionary of column_name:pyarrow_type
2.replace sanitize_row_for_arrow function with proper function which will set 
use code like
ddr_idx = table.schema.get_field_index("<col>")
table_casted = table.set_column(
    addr_idx,
    "<col>",
    table["<col>"].cast(pa.string())
)
which will cast complete column and adjust the complete code accordingly.
you can also just piece of code which are needs to be replaced along with line number if complete code is too big.
Since the existing code does not properly cast the columns while converting into parquets.
3. Replace pandas anywhere possible except for reading

Here are targeted changes to return both a schema and a column→PyArrow-type map, to cast entire columns (not per-row), and to remove pandas from the write path while keeping it for reads.[1]

### Quick answer
- Update get_oracle_table_schema to return a tuple: (pa.Schema, Dict[column_name, pa.DataType]) and adjust call sites accordingly.[1]
- Drop sanitize_row_for_arrow and introduce a column-wise caster that uses Table.set_column + ChunkedArray.cast to align each column to the target schema.[1]
- Replace pandas usage in the write path by building a PyArrow Table directly from cursor rows and uploading it, keeping pandas only where it reads from Oracle or Parquet.[1]

### 1) Schema function return
- Replace the function signature and return with a tuple, and add the type map creation at the end.[1]

Code to replace the existing function definition of get_oracle_table_schema:

```python
# NEW FUNCTION 1: Schema Mapping from Oracle to Arrow
from typing import Tuple  # add this near other typing imports

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

    # Split schema.table if provided
    if '.' in table_name:
        schema_name, table_only = table_name.split('.', 1)
    else:
        schema_name = oracle_config.get('schema', 'PUBLIC')
        table_only = table_name

    fields: List[pa.Field] = []
    with connect_to_oracle(oracle_config) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_query, {'table_name': table_only.upper(), 'schema_name': schema_name.upper()})
            for row in cur.fetchall():
                col_name, data_type, precision, scale, nullable, data_length, char_length = row

                # Map Oracle data types to PyArrow types (unchanged mapping logic)
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
    for field in fields:
        if pa.types.is_date(field.type):
            logger.info(f"DATE column: {field.name} : {field.type}")
        elif pa.types.is_timestamp(field.type):
            logger.info(f"TIMESTAMP column: {field.name} : {field.type}")
    return schema, type_map
```

- Update the call site in oracle_to_minio_parquet: replace oracle_schema = get_oracle_table_schema(oracle_config, table_name) with oracle_schema, arrow_type_map = get_oracle_table_schema(oracle_config, table_name).[1]

### 2) Replace per-row sanitize with column casts
- Remove sanitize_row_for_arrow entirely and replace it with two functions: rows_to_arrow_table to construct a Table from the raw cursor rows and minimal coercions (mainly LOB handling), and cast_table_to_schema to cast each column using set_column and ChunkedArray.cast as requested.[1]

Add these two functions, and delete sanitize_row_for_arrow:

```python
def cast_table_to_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """
    Cast each column of 'table' to target_schema using column-wise casts and set_column.
    """
    for field in target_schema:
        idx = table.schema.get_field_index(field.name)
        if idx == -1:
            continue
        col = table[field.name]  # ChunkedArray
        if not col.type.equals(field.type):
            try:
                casted = col.cast(field.type)            # safe cast first
            except pa.ArrowInvalid:
                casted = col.cast(field.type, safe=False) # allow coercion if needed
            table = table.set_column(idx, field, casted)
    return table


def rows_to_arrow_table(
    rows: Sequence[Sequence[Any]],
    schema: pa.Schema,
    column_names: Sequence[str],
) -> pa.Table:
    """
    Build a PyArrow Table from DB rows with light coercion only for LOB/string/binary,
    and then perform schema-aligned column-wise casts.
    """
    if not rows:
        empty_arrays = [pa.array([], type=f.type) for f in schema]
        return pa.Table.from_arrays(empty_arrays, names=column_names)

    # Transpose rows to columns
    cols = list(zip(*rows))
    arrays: List[pa.Array] = []
    for i, field in enumerate(schema):
        name = field.name
        values = list(cols[i])

        # Minimal pre-coercion so Arrow can ingest types before casting
        if pa.types.is_string(field.type):
            coerced = []
            for v in values:
                if isinstance(v, LOB):
                    try:
                        raw = v.read()
                        coerced.append(raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw) if raw is not None else None)
                    except Exception:
                        coerced.append(None)
                elif isinstance(v, (bytes, bytearray)):
                    coerced.append(v.decode("utf-8", "ignore"))
                elif v is None:
                    coerced.append(None)
                else:
                    coerced.append(str(v))
            arr = pa.array(coerced, type=pa.string())
        elif pa.types.is_binary(field.type):
            coerced = []
            for v in values:
                if isinstance(v, LOB):
                    try:
                        coerced.append(v.read())
                    except Exception:
                        coerced.append(None)
                elif isinstance(v, (bytes, bytearray)):
                    coerced.append(bytes(v))
                elif v is None:
                    coerced.append(None)
                else:
                    coerced.append(str(v).encode("utf-8"))
            arr = pa.array(coerced, type=pa.binary())
        else:
            # Let Arrow infer then we will cast
            coerced = []
            for v in values:
                if isinstance(v, LOB):
                    try:
                        coerced.append(v.read())
                    except Exception:
                        coerced.append(None)
                else:
                    coerced.append(v)
            arr = pa.array(coerced)

        arrays.append(arr)

    table = pa.Table.from_arrays(arrays, names=column_names)
    return cast_table_to_schema(table, schema)
```

- This change exactly implements the requested set_column approach and ensures whole-column casting instead of ad-hoc per-value conversions.[1]

### 3) Replace pandas in write path
- Introduce a new uploader that accepts a PyArrow Table so pandas is not used for writing, but keep existing pandas reads in get_historic_load_status as-is.[1]

Add this new uploader (keep the old one for backward compatibility, but stop calling it from the main flow):

```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def upload_table_parquet(
    minio_client: MinioHandler,
    table: pa.Table,
    object_path: str,
    compression: str = "snappy"
) -> None:
    """
    Upload a PyArrow Table as parquet to MinIO without pandas.
    """
    try:
        if hasattr(minio_client, "upload_table"):
            return minio_client.upload_table(
                table=table,
                object_path=object_path,
                format="parquet",
                compression=compression
            )

        # Fallback to manual put_object
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=compression)
        data = buf.getvalue()
        if hasattr(minio_client, "put_object"):
            minio_client.put_object(object_path, data, len(data), content_type="application/octet-stream")
        else:
            raise RuntimeError("MinioHandler must provide upload_table(...) or put_object(...)")
    except Exception as e:
        logger.error(f"Failed to upload parquet table to {object_path}: {e}")
        raise
```

- This removes pandas from the write path entirely while preserving the ability to read with pandas where already used.[1]

### Call-site edits in oracle_to_minio_parquet
- Make these replacements inside oracle_to_minio_parquet where chunk processing and upload happen.[1]

Search for this block and remove the pandas usage:

```python
# NEW: Sanitize all rows for Arrow compatibility
sanitized_rows = []
for row in rows:
    sanitized_row = sanitize_row_for_arrow(row, oracle_schema)
    sanitized_rows.append(sanitized_row)

# Create DataFrame with sanitized data
df_chunk = pd.DataFrame(sanitized_rows, columns=column_names)

# NEW: Upload with explicit schema preservation
upload_df_parquet_with_schema(mclient, df_chunk, oracle_schema, object_name, compression=compression)
recs = len(df_chunk)
```

Replace it with the Arrow-native path:

```python
# Build Arrow Table from rows and cast columns to the Oracle-derived schema
table_chunk = rows_to_arrow_table(rows, oracle_schema, column_names)

# Upload Arrow Table
upload_table_parquet(mclient, table_chunk, object_name, compression=compression)

recs = table_chunk.num_rows
```

- Also update the earlier schema retrieval to unpack the tuple: oracle_schema, arrow_type_map = get_oracle_table_schema(oracle_config, table_name).[1]
- Remove the sanitize_row_for_arrow function definition entirely, since its logic is replaced by rows_to_arrow_table + cast_table_to_schema.[1]

### Minimal diff-style summary
- get_oracle_table_schema: change return type to Tuple[pa.Schema, Dict[str, pa.DataType]] and return (schema, {f.name: f.type}).[1]
- oracle_to_minio_parquet: unpack schema and map; replace sanitize_row_for_arrow usage with rows_to_arrow_table + upload_table_parquet; set recs = table_chunk.num_rows.[1]
- Remove sanitize_row_for_arrow; add cast_table_to_schema and rows_to_arrow_table helpers.[1]
- Add upload_table_parquet and stop calling upload_df_parquet_with_schema in the main flow (keep for compatibility elsewhere if needed).[1]

### Notes on pandas reads
- The existing pandas usage in get_historic_load_status for pd.read_sql and pd.read_parquet can remain, since reads are explicitly allowed.[1]
- No other pandas calls should remain in the extraction/upload path after the above edits.[1]

Citations:
[1] paste.txt https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/90742002/8083187d-7cdb-4226-b615-5f67ea53756c/paste.txt
