Here is a walkthrough of the script from top to bottom so you can see the responsibilities and how to modify each part.

***

## Imports and logging

```python
import argparse
import datetime
import logging
import sys
from dataclasses import dataclass
from typing import List, Optional, Dict

import yaml
import trino
```

- Standard libs: `argparse` (CLI args), `datetime`, `logging`, `sys`, `dataclasses`, `typing`.
- Third-party: `yaml` (read config), `trino` (Trino DB-API client).

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("minio_iceberg_ingester")
```

- Sets global logging level and format.
- `logger` is used throughout to log progress, errors, and debugging info.

***

## Config data classes

```python
@dataclass
class TrinoConfig:
    host: str
    port: int
    user: str
    catalog_iceberg: str
    catalog_staging: str
    schema_log: str
```

- Holds Trino connection and catalog names:
  - `catalog_iceberg`: main Iceberg catalog (`minio` in your case).
  - `catalog_staging`: Hive catalog used for staging tables (`hive`).
  - `schema_log`: schema where ingestion log table lives (e.g., `meta`).

```python
@dataclass
class MinioConfig:
    bucket: str
    base_prefix: str
    folder_date_format: str
```

- Holds MinIO object layout:
  - `bucket`: MinIO bucket.
  - `base_prefix`: prefix up to the table, e.g. `raw/table_name`.
  - `folder_date_format`: how folder names encode dates, e.g. `"%Y%m%d"`.

```python
@dataclass
class TableConfig:
    schema: str
    name: str
    key_columns: List[str]
    date_column: str
    full_load: bool
    delta_load: bool
```

- Describes one target Iceberg table:
  - `schema` and `name`: Iceberg table location.
  - `key_columns`: business keys (e.g. `["cif"]`).
  - `date_column`: incremental column (`"aud_dt"`).
  - `full_load`, `delta_load`: which modes to run.

***

## Utility helpers

```python
def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
```

- Reads YAML config and returns Python dict.
- Modify if you want validation/defaults here.

```python
def create_trino_conn(trino_cfg: TrinoConfig):
    return trino.dbapi.connect(
        host=trino_cfg.host,
        port=trino_cfg.port,
        user=trino_cfg.user,
        http_scheme="http",
    )
```

- Creates a Trino DB-API connection using info from `TrinoConfig`.
- Change auth here if you need TLS, password, JWT, etc.

```python
def fetchall(conn, sql: str, params: Optional[List] = None) -> List[tuple]:
    logger.debug("Executing SQL (fetchall): %s", sql)
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        return cur.fetchall()
    finally:
        cur.close()
```

- Helper to run a query and return all rows.
- Wraps cursor open/close.
- If you want streaming or chunking, change this.

```python
def execute(conn, sql: str, params: Optional[List] = None):
    logger.debug("Executing SQL (execute): %s", sql)
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        _ = cur.fetchall()  # Trino DB-API requires consuming results
    finally:
        cur.close()
```

- Helper for “just run this SQL” (DDL, MERGE, etc.).
- Still `fetchall()` because Trino’s DB-API expects result consumption.

***

## Ingestion log table management

```python
def ensure_log_table(conn, trino_cfg: TrinoConfig):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log (
        target_catalog VARCHAR,
        target_schema  VARCHAR,
        target_table   VARCHAR,
        folder_date    VARCHAR,
        mode           VARCHAR,
        status         VARCHAR,
        run_id         VARCHAR,
        processed_at   TIMESTAMP,
        row_count      BIGINT,
        error_message  VARCHAR
    )
    """
    execute(conn, sql)
```

- Ensures a log table exists (idempotent).
- Adjust columns if you need more metadata.

```python
def log_start(conn, trino_cfg, table_cfg, folder_date, mode, run_id):
    sql = f"""
    INSERT INTO {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log (
        target_catalog, target_schema, target_table,
        folder_date, mode, status, run_id, processed_at, row_count, error_message
    )
    VALUES (?, ?, ?, ?, ?, 'STARTED', ?, current_timestamp, NULL, NULL)
    """
    ...
    execute(conn, sql, params)
```

- Inserts an entry marking a folder as “STARTED”.
- Called before starting staging + MERGE for that folder.

```python
def log_success(...):
    sql = f"""
    UPDATE {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log
    SET status = 'SUCCESS',
        processed_at = current_timestamp,
        row_count = ?,
        error_message = NULL
    WHERE target_catalog = ?
      AND target_schema  = ?
      AND target_table   = ?
      AND folder_date    = ?
      AND mode           = ?
      AND run_id         = ?
      AND status         = 'STARTED'
    """
    ...
    execute(conn, sql, params)
```

- After a successful MERGE, marks that `run_id` + `folder_date` as `SUCCESS` and stores row count.

```python
def log_failure(...):
    sql = f"""
    UPDATE {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log
    SET status = 'FAILED',
        processed_at = current_timestamp,
        error_message = ?
    WHERE target_catalog = ?
      AND target_schema  = ?
      AND target_table   = ?
      AND folder_date    = ?
      AND mode           = ?
      AND run_id         = ?
      AND status = 'STARTED'
    """
    ...
    execute(conn, sql, params)
```

- If staging or MERGE fails, records failure and error message.

```python
def get_successful_folders(conn, trino_cfg, table_cfg, mode: str) -> List[str]:
    sql = f"""
    SELECT DISTINCT folder_date
    FROM {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log
    WHERE target_catalog = ?
      AND target_schema  = ?
      AND target_table   = ?
      AND mode           = ?
      AND status         = 'SUCCESS'
    """
    ...
    return [r[0] for r in rows]
```

- For a given table and mode (`FULL` or `DELTA`), returns list of folder dates that were successfully processed.
- Used to skip already-processed folders.

***

## Folder discovery

```python
def list_available_folders(conn, trino_cfg: TrinoConfig,
                           minio_cfg: MinioConfig) -> List[str]:
    ...
    sql = f"""
    SELECT DISTINCT regexp_extract(path, '.*/(\\d{{8}})/.*', 1) AS folder
    FROM {trino_cfg.catalog_staging}.system.files
    WHERE path LIKE 's3a://{minio_cfg.bucket}/{minio_cfg.base_prefix}/%'
      AND regexp_extract(path, '.*/(\\d{{8}})/.*', 1) IS NOT NULL
    """
    rows = fetchall(conn, sql)
    folders = sorted({r[0] for r in rows})
    return folders
```

- Idea: “scan all file paths under `bucket/base_prefix/` and pull out any `\d{8}` segment as folder date”.
- The `system.files` part is pseudo / environment-dependent. Replace with:
  - A real Trino system table if you have it.
  - Or replace entirely with MinIO client calls listing prefixes.
- Returns list of folder names like `["20251225", "20251226", ...]`.

***

## Target schema discovery

```python
def get_target_schema(conn, trino_cfg: TrinoConfig,
                      table_cfg: TableConfig) -> List[Dict]:
    sql = f"""
    SELECT column_name, data_type
    FROM {trino_cfg.catalog_iceberg}.information_schema.columns
    WHERE table_schema = ?
      AND table_name   = ?
    ORDER BY ordinal_position
    """
    ...
    return [{"column_name": r[0], "data_type": r[1]} for r in rows]
```

- Reads Iceberg table schema from `information_schema`.
- Used to:
  - Create staging table with same columns & types.
  - Build MERGE statements (update set, insert columns).

***

## Staging table functions

```python
def build_staging_table_name(table_cfg: TableConfig, folder_date: str) -> str:
    return f"stg_{table_cfg.name}_{folder_date}"
```

- Deterministic name per table + folder.
- Makes cleanup/logging easy.

```python
def create_staging_table(conn, trino_cfg, minio_cfg, table_cfg,
                         folder_date, target_schema_cols):
    staging_table_name = build_staging_table_name(table_cfg, folder_date)

    column_ddls = []
    for col in target_schema_cols:
        col_name = col["column_name"]
        col_type = col["data_type"]
        column_ddls.append(f"{col_name} {col_type}")
    columns_ddl = ",\n        ".join(column_ddls)

    folder_path = f"s3a://{minio_cfg.bucket}/{minio_cfg.base_prefix}/{folder_date}/"

    sql_drop = f"DROP TABLE IF EXISTS {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}"
    execute(conn, sql_drop)

    sql_create = f"""
    CREATE TABLE {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name} (
        {columns_ddl}
    )
    WITH (
        external_location = '{folder_path}',
        format = 'PARQUET'
    )
    """
    execute(conn, sql_create)
    ...
```

- Drops any existing staging table with same name.
- Creates a Hive external table:
  - Location is exactly the folder for that date.
  - Format is Parquet.
  - Columns mirror the Iceberg target.
- If you want to add only a subset of columns, tweak `column_ddls` generation.

```python
def drop_staging_table(conn, trino_cfg, table_cfg, folder_date: str):
    staging_table_name = build_staging_table_name(table_cfg, folder_date)
    sql_drop = f"DROP TABLE IF EXISTS {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}"
    execute(conn, sql_drop)
```

- Cleans up staging table when done (success or failure).

```python
def validate_staging_date(conn, trino_cfg, table_cfg, folder_date,
                          date_column, folder_date_format):
    staging_table_name = build_staging_table_name(table_cfg, folder_date)
    iso_date = datetime.datetime.strptime(folder_date, folder_date_format).date().isoformat()

    sql = f"""
    SELECT DISTINCT {date_column}
    FROM {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}
    """
    rows = fetchall(conn, sql)
    distinct_dates = {str(r[0]) for r in rows}

    if len(distinct_dates) == 0:
        raise RuntimeError(f"No rows found in staging for folder {folder_date}")

    if distinct_dates != {iso_date}:
        raise RuntimeError(
            f"Staging {date_column} mismatch for folder {folder_date}: "
            f"found {distinct_dates}, expected only {iso_date}"
        )
```

- Guards against data misplacement:
  - Ensures every row’s `aud_dt` matches the folder’s encoded date.
  - If not, fails the run so you don’t merge the wrong date into wrong partition.
- Modify or remove if you want more relaxed validation.

***

## MERGE into Iceberg

```python
def run_merge(conn, trino_cfg, table_cfg, folder_date,
              folder_date_format, target_schema_cols) -> int:
    staging_table_name = build_staging_table_name(table_cfg, folder_date)

    iso_date = datetime.datetime.strptime(folder_date, folder_date_format).date().isoformat()

    key_eqs = [f"t.{k} = s.{k}" for k in table_cfg.key_columns]
    on_clause = " AND ".join(key_eqs)

    date_col = table_cfg.date_column
    on_clause += f" AND t.{date_col} = s.{date_col}"
```

- Builds `ON` clause for MERGE:
  - Business key equality (e.g., `t.cif = s.cif`).
  - Date equality (`aud_dt` matches on both sides).
- You can change this if you want different “latest wins” semantics.

```python
    key_set = set(table_cfg.key_columns + [date_col])
    non_key_cols = [c["column_name"] for c in target_schema_cols if c["column_name"] not in key_set]
    set_clauses = [f"{col} = s.{col}" for col in non_key_cols]
    set_sql = ", ".join(set_clauses) if set_clauses else ""
```

- Chooses which columns to update:
  - Excludes keys and `aud_dt`.
  - Everything else gets updated from staging (`col = s.col`).
- If you want to also update `aud_dt`, remove it from `key_set`.

```python
    all_cols = [c["column_name"] for c in target_schema_cols]
    insert_cols_sql = ", ".join(all_cols)
    insert_vals_sql = ", ".join([f"s.{c}" for c in all_cols])
```

- Build insert column list and values list:
  - Insert all columns from staging when the key is not matched.

```python
    merge_sql = f"""
    MERGE INTO {trino_cfg.catalog_iceberg}.{table_cfg.schema}.{table_cfg.name} t
    USING (
        SELECT *
        FROM {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}
        WHERE {date_col} = DATE '{iso_date}'
    ) s
    ON {on_clause}
    {"WHEN MATCHED THEN UPDATE SET " + set_sql if set_sql else ""}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols_sql})
        VALUES ({insert_vals_sql})
    """
```

- Main MERGE statement:
  - Reads from staging (filtered to that date).
  - `ON` defines upsert match.
  - Updates non-key columns, inserts new rows.

```python
    execute(conn, merge_sql)

    count_sql = f"""
    SELECT COUNT(*)
    FROM {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}
    WHERE {date_col} = DATE '{iso_date}'
    """
    rows = fetchall(conn, count_sql)
    row_count = rows[0][0] if rows else 0
    return row_count
```

- Executes the MERGE.
- Then counts how many rows were in staging (used as a metric for logging).

***

## Selecting folders for full vs delta

```python
def pick_folders_to_process(
    all_folders,
    success_folders_full,
    success_folders_delta,
    table_cfg: TableConfig
) -> Dict[str, List[str]]:
    result = {"full": [], "delta": []}

    if table_cfg.full_load:
        remaining = sorted(set(all_folders) - set(success_folders_full))
        result["full"] = remaining

    if table_cfg.delta_load and all_folders:
        latest = max(all_folders)
        if latest not in success_folders_delta:
            result["delta"] = [latest]

    return result
```

- `all_folders`: everything discovered in MinIO (e.g. `20251225`, `20251226`).
- `success_folders_full`: already-processed in FULL mode.
- `success_folders_delta`: already-processed in DELTA mode.
- If `full_load: true`, it selects all unprocessed folders (sorted).
- If `delta_load: true`, it picks the latest folder if not already done.
- You can easily change:
  - DELTA to be “latest N folders”.
  - FULL to restrict to up to some date.

***

## The main orchestration per table

```python
def run_for_table(conn, trino_cfg, minio_cfg, table_cfg):
    ensure_log_table(conn, trino_cfg)

    all_folders = list_available_folders(conn, trino_cfg, minio_cfg)
    ...
    success_full = get_successful_folders(conn, trino_cfg, table_cfg, mode="FULL")
    success_delta = get_successful_folders(conn, trino_cfg, table_cfg, mode="DELTA")

    todo = pick_folders_to_process(all_folders, success_full, success_delta, table_cfg)
```

- Makes sure log table exists.
- Finds all folders under prefix.
- Retrieves which folders have already succeeded in FULL and DELTA.
- Decides which folders to run in each mode.

```python
    target_schema_cols = get_target_schema(conn, trino_cfg, table_cfg)
    if not target_schema_cols:
        raise RuntimeError(...)
```

- Reads Iceberg schema once per run.
- Fails if the table has no schema.

### FULL loop

```python
    for folder in todo["full"]:
        run_id = f"FULL-{folder}"
        mode = "FULL"
        logger.info("Starting FULL load for folder %s", folder)
        try:
            log_start(...)
            create_staging_table(...)
            validate_staging_date(...)
            row_count = run_merge(...)
            log_success(...)
        except Exception as e:
            logger.exception("FULL load failed for folder %s", folder)
            log_failure(...)
        finally:
            drop_staging_table(...)
```

- For each folder needing FULL:
  - Logs `STARTED`.
  - Creates staging table on that folder.
  - Validates that `aud_dt` matches folder.
  - Runs MERGE.
  - Logs `SUCCESS` with row count, or `FAILED` with error.
  - Always drops staging table at end.

### DELTA loop

```python
    for folder in todo["delta"]:
        run_id = f"DELTA-{folder}"
        mode = "DELTA"
        logger.info("Starting DELTA load for folder %s", folder)
        try:
            log_start(...)
            create_staging_table(...)
            validate_staging_date(...)
            row_count = run_merge(...)
            log_success(...)
        except Exception as e:
            logger.exception("DELTA load failed for folder %s", folder)
            log_failure(...)
        finally:
            drop_staging_table(...)
```

- Same pattern, but only for the “latest unprocessed” folder.

***

## CLI entry point

```python
def parse_args():
    p = argparse.ArgumentParser(description="Ingest MinIO Parquet folders into Iceberg via Trino.")
    p.add_argument("--config", required=True, help="Path to YAML config")
    return p.parse_args()
```

- Simple CLI: you run `python ingest_minio_to_iceberg.py --config config.yaml`.

```python
def main():
    args = parse_args()
    cfg = load_yaml(args.config)

    trino_cfg = TrinoConfig(...)
    minio_cfg = MinioConfig(...)
    table_cfg = TableConfig(...)
```

- Loads YAML and maps it into the three config dataclasses.

```python
    try:
        conn = create_trino_conn(trino_cfg)
        run_for_table(conn, trino_cfg, minio_cfg, table_cfg)
    except Exception as e:
        logger.exception("Fatal error in ingestion")
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass
```

- Opens Trino connection.
- Runs the table ingestion logic.
- On any uncaught exception, logs it and exits with non-zero code.
- Always attempts to close the connection.

```python
if __name__ == "__main__":
    main()
```

- Standard Python entry point.

***

If you tell what you want to change first (e.g., different “latest wins” semantics, multiple tables in one YAML, or changing folder discovery to call MinIO API), a focused diff can be sketched for those sections specifically.
