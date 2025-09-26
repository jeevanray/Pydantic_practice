You're absolutely right! Let me provide a clean PyArrow-only approach that creates the table directly from cursor results and then applies columnar sanitization and casting.[1]

## **Pure PyArrow Approach - No Pandas**

### **1. Updated get_oracle_table_schema (return tuple)**
```python
def get_oracle_table_schema(oracle_config: Dict[str, Any], table_name: str) -> Tuple[pa.Schema, Dict[str, pa.DataType]]:
    """
    Fetch table schema and return (schema, type_map).
    """
    # ... existing schema query logic stays the same ...
    
    schema = pa.schema(fields)
    type_map: Dict[str, pa.DataType] = {f.name: f.type for f in fields}
    return schema, type_map
```

### **2. Create table directly from cursor + columnar sanitization**
```python
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
    
    # Build arrays column by column
    arrays = []
    for i, (column_data, field) in enumerate(zip(columns_data, target_schema)):
        array = sanitize_column_for_arrow(list(column_data), field)
        arrays.append(array)
    
    # Create table and cast to target schema
    table = pa.Table.from_arrays(arrays, names=column_names)
    return cast_table_to_target_schema(table, target_schema)

def sanitize_column_for_arrow(column_values: List[Any], field: pa.Field) -> pa.Array:
    """
    Sanitize a single column's values. Only handle special cases like LOBs.
    Let Arrow handle the rest through casting.
    """
    sanitized_values = []
    
    for value in column_values:
        if value is None:
            sanitized_values.append(None)
        elif isinstance(value, LOB):
            # Handle LOB types
            try:
                lob_data = value.read()
                if pa.types.is_string(field.type):
                    # CLOB -> string
                    if isinstance(lob_data, bytes):
                        sanitized_values.append(lob_data.decode('utf-8', errors='ignore'))
                    else:
                        sanitized_values.append(str(lob_data) if lob_data is not None else None)
                elif pa.types.is_binary(field.type):
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
        else:
            # For non-LOB values, pass through as-is
            # Let Arrow casting handle the type conversion
            sanitized_values.append(value)
    
    # Create array - let Arrow infer type, we'll cast later
    return pa.array(sanitized_values)

def cast_table_to_target_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """
    Cast each column using your requested approach: table.set_column + cast.
    """
    for field in target_schema:
        if field.name not in table.column_names:
            continue
            
        col_idx = table.schema.get_field_index(field.name)
        current_column = table[field.name]  # ChunkedArray
        
        # Only cast if types don't match
        if not current_column.type.equals(field.type):
            try:
                # Try safe cast first
                casted_column = current_column.cast(field.type, safe=True)
            except pa.ArrowInvalid:
                try:
                    # Fall back to unsafe cast for type coercion
                    casted_column = current_column.cast(field.type, safe=False)
                except Exception as e:
                    logger.warning(f"Failed to cast column {field.name} from {current_column.type} to {field.type}: {e}")
                    continue
            
            # Replace the column using your requested approach
            table = table.set_column(col_idx, field.name, casted_column)
    
    return table
```

### **3. Pure PyArrow upload (no pandas)**
```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def upload_arrow_table_parquet(minio_client: MinioHandler, table: pa.Table, 
                               object_path: str, compression: str = "snappy") -> None:
    """
    Upload PyArrow Table directly as parquet. No pandas involved.
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
```

### **4. Updated main processing in oracle_to_minio_parquet**
Replace the pandas chunk processing with:

```python
# Get schema with type map
oracle_schema, arrow_type_map = get_oracle_table_schema(oracle_config, table_name)

# In the chunk processing loop:
while True:
    rows = cursor.fetchmany(chunk_size)
    if not rows:
        break
    
    # Create Arrow table directly from rows - no pandas!
    table_chunk = create_arrow_table_from_rows(rows, column_names, oracle_schema)
    
    # Upload directly
    upload_arrow_table_parquet(mclient, table_chunk, object_name, compression=compression)
    
    recs = table_chunk.num_rows
    # ... rest of loop logic
```

## **Why This Approach Works Better**

1. **Direct table creation** - No row-by-row sanitization, build table directly from cursor results[1]
2. **Columnar LOB handling** - Only process LOBs column-wise where needed, everything else passes through[1]
3. **Your exact casting approach** - Uses `table.set_column()` and `column.cast()` as you requested[1]
4. **No pandas** - Pure PyArrow from cursor to parquet upload[1]
5. **Handles empty columns** - Arrow's casting with safe/unsafe fallback handles null columns properly[1]

The key insight: let Oracle cursor + PyArrow handle most type conversions automatically, only intervene for LOBs and final schema alignment. This is much simpler and more robust than row-by-row processing![1]

[1](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/90742002/8083187d-7cdb-4226-b615-5f67ea53756c/paste.txt)
