After analyzing your code, I found the issue with **total execution time not accumulating properly**. The problem lies in how the execution time is handled in the final completion phase versus how total records are handled.

## The Issue

The code correctly accumulates both `total_records` and `task_exec_secs` during chunk processing, but **overwrites the cumulative execution time** in the final completion phase.

### During Chunk Processing (Working Correctly)
```python
# Calculate incremental execution time for this chunk
current_run_time = (now_ist - extraction_start).total_seconds()
previous_exec_time = float(audit_log.get("task_exec_secs", 0))
cumulative_exec_time = previous_exec_time + current_run_time

audit_log.update({
    "total_records": cumulative_total,  # ✅ Cumulative total preserved
    "task_exec_secs": cumulative_exec_time,  # ✅ Cumulative time preserved
    "extraction_time": cumulative_exec_time,
    # ... other fields
})
```

### In Final Completion Phase (The Problem)
```python
# Finalize audit on success
final_ist = datetime.now(IST)
audit_log.update({
    "status": "COMPLETED",
    "task_endts": final_ist.strftime(DATETIMEFORMAT),
    "task_exec_secs": (final_ist - extraction_start).total_seconds(),  # ❌ OVERWRITES cumulative time
    "extraction_time": (final_ist - extraction_start).total_seconds(),  # ❌ OVERWRITES cumulative time
})
```

## The Fix

Replace the final completion phase with this corrected version:

```python
# Finalize audit on success
final_ist = datetime.now(IST)
current_delta_time = (final_ist - extraction_start).total_seconds()
previous_exec_time = float(audit_log.get("task_exec_secs", 0))

# For historic loads with multiple deltas, preserve cumulative time
if load_type == 'historic':
    # Get the cumulative time from the last chunk processing update
    cumulative_time = max(previous_exec_time, current_delta_time)
else:
    # For regular delta loads, use current run time
    cumulative_time = current_delta_time

audit_log.update({
    "status": "COMPLETED",
    "task_endts": final_ist.strftime(DATETIMEFORMAT),
    "task_exec_secs": cumulative_time,  # ✅ Preserve cumulative time
    "extraction_time": cumulative_time,  # ✅ Preserve cumulative time
})
```

## Why Total Records Work But Execution Time Doesn't

1. **Total Records**: The `total_records` field is only updated during chunk processing and **never overwritten** in the final phase
2. **Execution Time**: The `task_exec_secs` field is correctly accumulated during chunk processing but then **completely overwritten** in the final completion phase with just the current delta's execution time

## Root Cause

The final completion phase calculates `(final_ist - extraction_start).total_seconds()` which only represents the time elapsed for the **current delta**, not the cumulative time across all deltas. This overwrites the carefully accumulated cumulative time that was built up during chunk processing.

The fix ensures that the cumulative execution time built during chunk processing is preserved in the final audit record, just like how total records are handled.

[1](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/90742002/a2bd399f-8677-4b3d-988c-38ead29e5e38/paste.txt)
