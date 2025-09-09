The issue is that the `total_records` field in the audit table is not accumulating across different `delta_column_value` runs for historic loads. Instead, it’s being reset or overwritten with the current delta’s record count. The `oracle_to_minio_parquet` function partially handles cumulative totals but fails to correctly accumulate `total_records` across all delta values because the audit record is updated per delta without preserving the cumulative sum from previous deltas. The `initialize_restart_audit_log` function also doesn’t fetch cumulative totals correctly, as its query groups by `delta_column_value`, preventing a true sum across all deltas.

To fix this, we need to modify the `initialize_restart_audit_log` function to fetch the cumulative `total_records` across all historic load runs for the same `source_table` and update the audit record update logic in `oracle_to_minio_parquet` to ensure `total_records` accumulates correctly.

### Changes Required

#### 1. Modify `initialize_restart_audit_log` (Lines 108–192)
Update the cumulative query to sum `total_records` across all `delta_column_value` entries for the `source_table` with `load_type='historic'`, and ensure the main query uses this cumulative total.

**Replace Lines 108–192 with:**

```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry=retry_if_exception_type(Exception))
def initialize_restart_audit_log(config_audit: Dict[str, Any], audit_log: Dict[str, Any], aud_dt: str,
                                delta_column_value: Optional[str] = None) -> None:
    """Load existing audit record for restart - ensures single record per job and handles cumulative values."""
    
    # Cumulative query to get total_records across all deltas for historic loads
    cumulative_query = f"""
        SELECT
            source_table,
            SUM(total_records) as cumulative_total_records,
            SUM(task_exec_secs) as cumulative_exec_secs,
            SUM(extraction_time) as cumulative_extraction_time
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE source_table = :src
        AND load_type = 'historic'
        GROUP BY source_table
    """
    
    # Get most recent record and cumulative stats
    if audit_log.get("load_type") == "historic" and delta_column_value:
        query = f"""
            SELECT
                a.source_table, a.task_startts, a.task_endts, a.task_exec_secs, a.business_loaddt,
                a.delta_column_value, a.total_records, a.extraction_time, a.total_apicalls,
                a.success_apicalls, a.failed_apicalls, a.api_failedpath, a.apicall_time,
                a.cdp_db_count_validation, a.aerospike_init_record_cnt, a.aerospike_init_read_waittime,
                a.aerospike_record_cnt, a.aerospike_waittime, a.aerospike_error,
                a.mongodb_init_record_cnt, a.mongodb_record_cnt, a.mongodb_init_read_waittime,
                a.mongodb_waittime, a.mongodb_error, a.suspected_updates_or_blacklisted_records,
                a.difference_aero_mongo, a.status, a.log_path, a.minio_filepath, a.restart_point,
                a.load_type,
                c.cumulative_total_records, c.cumulative_exec_secs, c.cumulative_extraction_time
            FROM {config_audit["schema"]}.{config_audit["audit_table"]} a
            LEFT JOIN ({cumulative_query}) c
            ON a.source_table = c.source_table
            WHERE a.source_table = :src
            AND a.delta_column_value = :delta_val
            AND a.load_type = 'historic'
            ORDER BY a.updated_at_ts DESC NULLS LAST
            FETCH FIRST 1 ROW ONLY
        """
        params = {"src": audit_log["source_table"],
                  "delta_val": delta_column_value
                  }
    else:
        query = f"""
            SELECT
                a.source_table, a.task_startts, a.task_endts, a.task_exec_secs, a.business_loaddt,
                a.delta_column_value, a.total_records, a.extraction_time, a.total_apicalls,
                a.success_apicalls, a.failed_apicalls, a.api_failedpath, a.apicall_time,
                a.cdp_db_count_validation, a.aerospike_init_record_cnt, a.aerospike_init_read_waittime,
                a.aerospike_record_cnt, a.aerospike_waittime, a.aerospike_error,
                a.mongodb_init_record_cnt, a.mongodb_record_cnt, a.mongodb_init_read_waittime,
                a.mongodb_waittime, a.mongodb_error, a.suspected_updates_or_blacklisted_records,
                a.difference_aero_mongo, a.status, a.log_path, a.minio_filepath, a.restart_point,
                a.load_type,
                c.cumulative_total_records, c.cumulative_exec_secs, c.cumulative_extraction_time
            FROM {config_audit["schema"]}.{config_audit["audit_table"]} a
            LEFT JOIN ({cumulative_query}) c
            ON a.source_table = c.source_table
            WHERE a.source_table = :src
            AND a.business_loaddt = TO_DATE(:aud_dt, :fmt)
            AND (
                (a.delta_column_value IS NULL AND :delta_val IS NULL)
                OR a.delta_column_value = :delta_val
            )
            AND NVL(a.load_type, 'delta') = :load_type
            ORDER BY a.updated_at_ts DESC NULLS LAST
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
                    "load_type", "cumulative_total_records", "cumulative_exec_secs", "cumulative_extraction_time"
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
                
                # Use cumulative total_records if available
                if existing_data.get("cumulative_total_records") is not None:
                    audit_log["total_records"] = int(existing_data["cumulative_total_records"] or 0)
                    logger.info("Loaded cumulative total_records: %d", audit_log["total_records"])
                
                logger.info("Existing audit record found - restart_point=%s status=%s delta_value=%s cumulative_records=%s",
                           audit_log.get("restart_point"), audit_log.get("status"),
                           audit_log.get("delta_column_value"), audit_log.get("total_records"))
            else:
                logger.info("No existing audit record found - starting fresh")
```

**Explanation**:
- The `cumulative_query` now sums `total_records`, `task_exec_secs`, and `extraction_time` across all historic load records for the `source_table`, regardless of `delta_column_value`.
- The main query joins with this to fetch the cumulative `total_records` and sets it in `audit_log["total_records"]` when a record exists.
- This ensures `total_records` starts with the sum of all previous deltas’ records.

#### 2. Modify `oracle_to_minio_parquet` (Lines 882–904, new delta block)
Update the new delta block to use the cumulative `total_records` and avoid resetting it.

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
- Ensures `total_records` is not reset to 0 when starting a new delta; it uses the cumulative total loaded from `initialize_restart_audit_log`.
- Preserves `task_exec_secs` and `extraction_time` to maintain continuity.

#### 3. Modify `oracle_to_minio_parquet` Chunk Loop (Lines 964–983)
Update the chunk loop to correctly accumulate `total_records` and execution times.

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
- Uses `len(df_chunk)` for `current_run_recs` to ensure accurate record counting.
- Adds `current_run_recs` to `previous_total` to maintain a cumulative `total_records`.
- Accumulates `task_exec_secs` and `extraction_time` incrementally to reflect total time across chunks and deltas.

#### 4. Fix Variable Reference (Line 987)
The line `total_records = proposed_total` references an undefined variable. Replace it with the correct variable.

**Replace Line 987 with:**

```python
            total_records = cumulative_total  # Update with cumulative total
```

**Explanation**:
- Corrects the variable reference to use `cumulative_total` from the audit update.

### Notes
- **Impact**: These changes ensure `total_records` accumulates across all `delta_column_value` runs for a `source_table` with `load_type='historic'`. The `initialize_restart_audit_log` function fetches the sum of `total_records` from all historic load records, and the chunk loop adds to this cumulative total.
- **Testing**: Verify by running a historic load with multiple `delta_column_value` values (e.g., `2023-05-14`, `2023-05-15`). Check the audit table to confirm `total_records` reflects the sum of records across all deltas, not just the current delta.
- **Debugging**: Add logging in the chunk loop to confirm `current_run_recs > 0`. If `total_records` still doesn’t increase, ensure `df_chunk` contains rows (i.e., the query is returning data).
- **No Other Changes**: The rest of the code (e.g., skipping logic, MinIO uploads) remains unchanged as it’s working correctly per your description.
