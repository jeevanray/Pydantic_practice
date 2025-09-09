You're correct that if the audit table maintains only one record per `source_table` for `load_type='historic'`, there's no need to aggregate values in `initialize_restart_audit_log` using a cumulative query. Instead, we should simply fetch the single existing record for the `source_table` and use its `total_records`, `task_exec_secs`, and `extraction_time` as the starting point, then add new values to these fields during updates in `oracle_to_minio_parquet`. This avoids unnecessary aggregation and ensures the audit record accumulates `total_records` and execution times correctly by updating the existing values rather than replacing them.

The issue in the provided code is that the `initialize_restart_audit_log` function uses a complex query with a cumulative subquery that's unnecessary for a single-record setup. Additionally, the `oracle_to_minio_parquet` function resets `total_records` and execution times when starting a new `delta_column_value`, which prevents proper accumulation.

Below is the corrected version of the `initialize_restart_audit_log` function and the relevant update section in `oracle_to_minio_parquet` to fetch the single record's values and accumulate `total_records`, `task_exec_secs`, and `extraction_time` correctly.

### Changes Required

#### 1. Modify `initialize_restart_audit_log` (Replace Lines 1–97 from your snippet)
Simplify the function to fetch the single audit record for the `source_table` with `load_type='historic'` without aggregation, and load its `total_records`, `task_exec_secs`, and `extraction_time` directly.

```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def initialize_restart_audit_log(config_audit: Dict[str, Any], audit_log: Dict[str, Any], aud_dt: str,
                                delta_column_value: Optional[str] = None) -> None:
    """Load existing audit record for restart - fetches single record for historic loads."""
    
    # Query to fetch the single audit record for the source_table
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
                audit_log.update(existing_data)
                
                # Handle CLOB fields
                try:
                    from oracledb import LOB
                    for field in ["api_failedpath", "aerospike_error", "mongodb_error"]:
                        if isinstance(audit_log.get(field), LOB):
                            audit_log[field] = audit_log[field].read()
                except Exception:
                    pass
                
                logger.info("Existing audit record found - restart_point=%s status=%s delta_value=%s total_records=%s",
                           audit_log.get("restart_point"), audit_log.get("status"),
                           audit_log.get("delta_column_value"), audit_log.get("total_records"))
            else:
                logger.info("No existing audit record found - starting fresh")
```

**Explanation**:
- For `load_type='historic'`, the query fetches the single audit record for the `source_table` without filtering by `delta_column_value`, as there’s only one record.
- The fetched `total_records`, `task_exec_secs`, and `extraction_time` are loaded into `audit_log` as the starting point.
- The delta load case remains unchanged to maintain compatibility.

#### 2. Modify `oracle_to_minio_parquet` (Lines 882–904, new delta block)
Update the new delta block to preserve and accumulate `total_records`, `task_exec_secs`, and `extraction_time` from the existing audit record.

**Replace Lines 882–904 with:**

```python
            # Previous delta completed, append to completed_deltas and start new delta
            logger.info("Previous delta %s completed, starting new delta %s", audit_log.get("delta_column_value"), delta_column_value)
            previous_delta = audit_log.get("delta_column_value")
            completed = audit_log.get("aerospike_error") or ""
            audit_log["aerospike_error"] = completed + ("," if completed else "") + str(previous_delta or "")
            
            # Preserve cumulative totals for the new delta
            previous_total = int(audit_log.get("total_records", 0))
            previous_exec_time = float(audit_log.get("task_exec_secs", 0))
            
            audit_log["delta_column_value"] = delta_column_value
            audit_log["business_loaddt"] = delta_column_value
            audit_log["status"] = "RUNNING"
            audit_log["restart_point"] = 0
            audit_log["task_startts"] = extraction_start.strftime(DATETIMEFORMAT)
            # Preserve cumulative values
            audit_log["total_records"] = previous_total  # Keep cumulative total
            audit_log["task_exec_secs"] = previous_exec_time
            audit_log["extraction_time"] = previous_exec_time
            
            logger.info("AUDIT: Preserving cumulative totals - Records: %d, Execution time: %.2f seconds",
                       previous_total, previous_exec_time)
            
            update_audit_record_strict(config_audit, audit_log)
            start_chunk_index = 0
            total_records = previous_total  # Start from cumulative total
```

**Explanation**:
- Retains the existing `total_records`, `task_exec_secs`, and `extraction_time` from the audit record instead of resetting them.
- Updates `delta_column_value` and `business_loaddt` for the new delta while keeping cumulative values intact.

#### 3. Modify `oracle_to_minio_parquet` Chunk Loop (Lines 964–983)
Update the chunk loop to add new records and execution time to the existing totals.

**Replace Lines 964–983 with:**

```python
            # Prepare audit update with cumulative totals
            now_ist = datetime.now(IST)
            current_run_recs = len(df_chunk)  # Records in this chunk
            previous_total = int(audit_log.get("total_records", 0))  # Previous cumulative total
            cumulative_total = previous_total + current_run_recs
            
            # Calculate incremental execution time for this chunk
            current_run_time = (now_ist - extraction_start).total_seconds()
            previous_exec_time = float(audit_log.get("task_exec_secs", 0))
            cumulative_exec_time = previous_exec_time + current_run_time

            logger.info("PROGRESS: Chunk %d - Current records: %d, Cumulative total: %d records", 
                       chunk_index, current_run_recs, cumulative_total)
            
            audit_log.update({
                "total_records": cumulative_total,
                "status": "RUNNING",
                "task_endts": now_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": cumulative_exec_time,
                "extraction_time": cumulative_exec_time,  # Keep both times in sync
                "restart_point": chunk_index + 1,
                "minio_filepath": effective_object_path,
            })
```

**Explanation**:
- Adds `current_run_recs` (from `len(df_chunk)`) to `previous_total` to accumulate `total_records`.
- Adds `current_run_time` to `previous_exec_time` to accumulate `task_exec_secs` and `extraction_time`.

#### 4. Fix Variable Reference (Line 987)
Correct the undefined `proposed_total` variable.

**Replace Line 987 with:**

```python
            total_records = cumulative_total  # Update with cumulative total
```

**Explanation**:
- Uses `cumulative_total` to update `total_records` after a successful audit update.

### Additional Notes
- **Single Record Assumption**: These changes assume the audit table has only one record per `source_table` for `load_type='historic'`, as you indicated. The `initialize_restart_audit_log` function fetches this record directly without aggregating across multiple records.
- **Testing**: Run a historic load for `UDS.RETAIL_GDM_CUST_DIM` with multiple `delta_column_value` values (e.g., `2023-05-14`, `2023-05-15`). Check the audit table to confirm that `total_records` increases with each delta (e.g., if `2023-05-14` processes 1000 rows and `2023-05-15` processes 500 rows, `total_records` should be 1500 after both). Verify `task_exec_secs` and `extraction_time` also accumulate.
- **Debugging**: If `total_records` still doesn’t increase, add a log statement before `len(df_chunk)` to confirm `rows` is not empty:
  ```python
  logger.debug("Fetched %d rows for chunk %d", len(rows), chunk_index)
  ```
  If `rows` is empty, check the `select_sql` query for issues (e.g., incorrect `delta_column_value` or no matching data).
- **No Other Changes**: Only the audit update logic is modified, as requested, to ensure the rest of the code (which is working) remains untouched.
- **Audit Table Constraint**: The existing `uk_audit_composite` constraint in `create_audit_table_if_not_exists` (Line 614) ensures a single record for `load_type='historic'` by making `business_loaddt` and `delta_column_value` NULL, enforcing one record per `source_table`. This aligns with your single-record assumption.
